import pickle
import uuid
from random import randint
import selectors
import socket
import logging
import json
import sys
import pathlib
import os
import time
import threading
import types
from queue import Queue
import enum
from typing import Dict

import rsa

from constants import *
from util_classes import *

logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)


class Client:
    EOM_IN_BYTES = pickle.dumps(Event.EOM)

    def __init__(self, client_id, clients_info):

        # config file state
        self.peer_next_log_index = dict()
        self.restoration_in_progress = dict()
        self.self_server_conn = None
        self.client_id = client_id
        self.clients_info = clients_info
        self.restoration_lock = threading.Lock()

        # application related state
        self.private_key, self.public_keys_dict = self.load_keys(self.client_id)

        # stores conn objs of all peers
        self.peer_conn_dict = dict()

        # load state from file if existed before
        self.state_file = 'raft-state-%s' % self.client_id
        self.load_state()
        self.entries = list()
        self.leader_id = None
        self.votes_recvd = 0
        self.role = Role.FOLLOWER

        # group attributes
        self.group_counter = 0

        self.server_flag = threading.Event()
        self.server_flag.set()

        self.heartbeat_event = threading.Event()
        self.election_decision_event = threading.Event()
        self.sent_heartbeat_event = threading.Event()

        # TODO: wtf to do about timeouts
        self.heartbeat_timeout = 5 + int(self.client_id) * 5
        self.election_timeout = 5 + int(self.client_id) * 5

        # start the client's peer server
        self.server_thread = threading.Thread(target=self.start_server_thread)
        self.server_thread.daemon = True
        self.server_thread.start()

        # connect to all the other clients
        time.sleep(15)
        self.connect_to_other_peers()
        self.connect_to_self_server()

        time.sleep(10)
        self.ping_clients_to_connect_to_server()
        time.sleep(5)

        # input("PRESS ANY KEY TO START LEADER ELECTION!!")

        state_thread = threading.Thread(target=self.start_state_thread)
        state_thread.daemon = True
        state_thread.start()

        time.sleep(5)
        self.start_ui()

    def load_state(self):
        # just to enable intellisense
        self.state = State(0, None, [LogEntry(0, 0, Command(0, 'START'))])
        try:
            with open(self.state_file, 'r') as f:
                self.state = pickle.load(f)
        except Exception as e:
            # self.state = State(0, None, list())
            logger.warning("No restoration file found!")
            pass

    @staticmethod
    def load_keys(client_idx):
        client_id_public_key_dict = dict()
        with open(KEYOBJECT_FILE_PATH, 'rb') as keystore:
            keys_dict = pickle.load(keystore)
            private_key = keys_dict[client_idx][1]
            for client_id, keys in keys_dict.items():
                client_id_public_key_dict[client_id] = keys[1]
        return private_key, client_id_public_key_dict

    @staticmethod
    def display_menu():
        logger.info("- Press 1 to CreateGroup")
        logger.info("- Press 2 to Add Member")
        logger.info("- Press 3 to Kick Member")
        logger.info("- Press 4 to Write Message")
        logger.info("- Press 5 to Print Group")
        logger.info("- Press 6 to Fail Link")
        logger.info("- Press 7 to Fix Link")
        logger.info("- Press 8 to Fail Process")
        # TODO : delete this later
        logger.info("- Press 9 to display logs")

    @staticmethod
    def accept_wrapper(sock, selector):
        conn, addr = sock.accept()  # Should be ready to read
        logger.info(f'accepted connection from : {addr}')
        conn.setblocking(False)
        data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        selector.register(conn, events, data=data)

    @staticmethod
    def get_request_list(data):
        byte_message_list = data.split(Client.EOM_IN_BYTES)
        req_list = []
        for msg in byte_message_list:
            if not msg:
                continue
            req = pickle.loads(msg)
            req_list.append(req)
        return req_list

    def service_connection(self, key, mask, selector):
        sock = key.fileobj
        data = key.data
        if mask & selectors.EVENT_READ:
            recv_data = sock.recv(102400)

            if recv_data:
                req_list = self.get_request_list(recv_data)
                for req in req_list:
                    self.handle_request_from_peer(req)
            else:
                logger.info('closing connection to : ' + str(data.addr))
                selector.unregister(sock)
                sock.close()

    def on_follower(self):
        # can go to candiate with timeout (otherwise will always be follower)

        while self.heartbeat_event.wait(timeout=self.heartbeat_timeout):
            logger.info("hearbeat recvd")
            self.heartbeat_event.clear()
            # repeat the waiting for heartbeat

        logger.info("becoming candidate as no heartbeat recvd")
        self.role = Role.CANDIDATE

    def on_candidate(self):
        # can only come from follower timeout

        # send request vote RPC to all peers
        self.request_vote_from_peers()

        while self.election_decision_event.wait(timeout=self.election_timeout) is False:
            logger.info("election decision not recieved")
            self.request_vote_from_peers()

        self.election_decision_event.clear()
        # the decision event itself would have set the role now

    def on_leader(self):
        logger.info("\n\n\n\nGUESSS WHO IS LEADER BITCHESSSSSS!!!!!\n\n\n")
        # send appendrpc to all peers in heartbeat time
        self.initialize_peer_next_log_index()
        self.send_append_entries_to_peers(last_log_index=self.state.get_last_log_index(),
                                          last_log_term=self.state.get_last_log_term())

        while self.role == Role.LEADER:
            # TODO:  fix the timeout value
            flag = self.sent_heartbeat_event.wait(timeout=5)
            # if has to step down during the timeout
            if self.role != Role.LEADER:
                break

            if flag:
                logger.info("someone sent append rpc - restart the sending of heartbeat")
            else:
                logger.info("time to send heartbeats")
                self.send_append_entries_to_peers(last_log_index=self.state.get_last_log_index(),
                                                  last_log_term=self.state.get_last_log_term())

            self.sent_heartbeat_event.clear()

        logger.info("the leader has to step down")
        # the step down event only would have set the role now

    def send_append_entries_to_peer(self, recipient_id, entries, peer_last_log_consistent_entry_index,
                                    peer_last_log_consistent_entry_term):
        request = {EVENT_KEY: Event.APPEND_ENTRY,
                   SENDER_ID: self.client_id,
                   TERM_KEY: self.state.curr_term,
                   PREV_LOG_IDX: peer_last_log_consistent_entry_index,
                   PREV_LOG_TERM: peer_last_log_consistent_entry_term,
                   ENTRIES: entries,
                   COMMIT_IDX: self.state.commit_idx}

        time.sleep(1)
        # TODO: implement retry logic for send failures
        logger.info("Trying to send Append entry to client to make logs consistent : ")
        self.send_msg_to_peer(recipient_id, request)

    def send_append_entries_to_peers(self, last_log_index, last_log_term, entries=None):

        if entries is None:
            entries = []
        request = {EVENT_KEY: Event.APPEND_ENTRY,
                   SENDER_ID: self.client_id,
                   TERM_KEY: self.state.curr_term,
                   PREV_LOG_IDX: last_log_index,
                   PREV_LOG_TERM: last_log_term,
                   ENTRIES: entries,
                   COMMIT_IDX: self.state.commit_idx}

        time.sleep(1)

        for peer_id in self.peer_conn_dict:
            logger.info(f"Sending append entries to client.")
            # self.send_msg_to_peer(peer_id, request)
            with self.restoration_lock:
                if self.restoration_in_progress[peer_id]:
                    logger.warning(
                        f"Not sending heartbeat to {peer_id}. Probably busy getting its logs in sync with leader..")
                    continue
            # if self.peer_next_log_index[peer_id] == last_log_index + 1:
            self.send_msg_to_peer(peer_id, request)
        self.sent_heartbeat_event.set()

    def request_vote_from_peers(self):
        self.votes_recvd = 1
        self.state.curr_term += 1
        self.voted_for = self.client_id
        request = {EVENT_KEY: Event.REQUEST_VOTE,
                   SENDER_ID: self.client_id,
                   TERM_KEY: self.state.curr_term,
                   LAST_LOG_IDX: self.state.get_last_log_index(),
                   LAST_LOG_TERM: self.state.get_last_log_term()}

        time.sleep(1)

        for peer_id in self.peer_conn_dict:
            logger.info(f"Sending vote request to client {peer_id} : " + str(request))
            self.send_msg_to_peer(peer_id, request)

    def start_state_thread(self):

        while True:
            if self.role == Role.FOLLOWER:
                self.on_follower()
            elif self.role == Role.CANDIDATE:
                self.on_candidate()
            elif self.role == Role.LEADER:
                self.on_leader()
            else:
                logger.info("Unknown role")

    def start_server_thread(self):
        server_host = self.clients_info[self.client_id]["host"]
        server_port = self.clients_info[self.client_id]["port"]
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        selector = selectors.DefaultSelector()
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((server_host, server_port))

        server_socket.listen()
        logger.info("Peer Server is up and running!")
        logger.info("Waiting on new connections...")
        server_socket.setblocking(False)
        selector.register(server_socket, selectors.EVENT_READ, data=None)

        while True and self.server_flag.is_set():
            events = selector.select(timeout=None)
            for key, mask in events:
                if key.data is None:
                    self.accept_wrapper(key.fileobj, selector)
                else:
                    self.service_connection(key, mask, selector)

    def start_ui(self):
        user_req_id = 0
        while True:
            user_req_id += 1
            self.display_menu()
            # Press 1 to CreateGroup")
            # Press 2 to Add Member")
            # Press 3 to Kick Member")
            # Press 4 to Write Message")
            # Press 5 to Print Group")
            # Press 6 to Fail Link")
            # Press 7 to Fix Link")
            # Press 8 to Fail Process")
            # Press 9 to display logs")
            user_input = input("Client prompt >> ").strip()
            if user_input == "1":
                self.handle_create_group()
            elif user_input == "2":
                # TODO: just testting append RPC for now
                self.send_new_command_to_leader()

            elif user_input == "3":
                # tell the snapshot thread by adding the event into the snapshot queue
                self.snapshot_thread_queue.put({"type": Event.INIT_SNAPSHOT, "balance": self.balance})
            elif user_input == "4":
                self.server_flag.clear()
                logger.info("Until next time...")
                break
            elif user_input == "9":
                # TODO: just testting append RPC for now
                logger.info("here are the logs : ")
                logger.info(self.state.log)
            else:
                logger.warning("Incorrect menu option. Please try again..")
                continue

    def connect_to_other_peers(self):
        # connect to all other peers
        for other_client_id, client_info in self.clients_info.items():
            if other_client_id == self.client_id:
                continue
            peer_addr = (client_info["host"], client_info["port"])
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            logger.info("Trying to connect to peer : " + other_client_id)
            try:
                sock.connect(peer_addr)
                logger.info("Now connected to peer : " + other_client_id)
                self.peer_conn_dict[other_client_id] = sock
            except Exception as ex:
                logger.error(f"Could not CONNECT to peer : {other_client_id}")
                logger.error(ex)

    def ping_clients_to_connect_to_server(self):
        request = {
            EVENT_KEY: Event.NEW_CONNECTION,
            SENDER_ID: self.client_id
        }
        for peer_id in self.peer_conn_dict:
            logger.info(f"Send NEW CONNECTION event to peer : {peer_id}")
            self.send_msg_to_peer(peer_id, request)

    def handle_request_from_peer(self, req):

        sender_id = req[SENDER_ID]

        if req[EVENT_KEY] == Event.NEW_CONNECTION:
            self.re_establish_connection(sender_id)

        elif req[EVENT_KEY] == Event.REQUEST_VOTE:
            self.handle_request_vote(req, sender_id)

        elif req[EVENT_KEY] == Event.RESPOND_VOTE:
            self.handle_respond_vote(req, sender_id)

        elif req[EVENT_KEY] == Event.APPEND_ENTRY:
            self.handle_append_entry(req, sender_id)

        elif req[EVENT_KEY] == Event.RESPOND_ENTRY:
            self.handle_respond_entry(req, sender_id)

        elif req[EVENT_KEY] == Event.NEW_COMMAND:
            self.handle_new_command(req, sender_id)

    def handle_new_command(self, req, sender_id):
        logger.info("NEW_COMMAND received from " + str(sender_id) + " " + str(req))
        command = req[COMMAND]
        request_id = req[REQUEST_ID]

        if self.role == Role.LEADER:
            with self.state.lock:
                if self.state.req_in_logs(request_id):
                    return
                log_entry = LogEntry(self.state.curr_term, self.state.get_last_log_index() + 1,
                                     Command(request_id, command))
                self.state.log.append(log_entry)
                # we cant maintain count as the same peer can vote twice, in case he goes down and comes back up
                self.state.client_req_respond_count[request_id] = {self.client_id}

            # no need to send append entry, will be taken care by the heartbeat thread.
            # self.send_append_entries_to_peers(last_log_index=last_log_idx, last_log_term=last_log_term,
            #                                   entries=[log_entry])

    def handle_respond_entry(self, req, sender_id):
        logger.info("RESPOND_ENTRY received from " + str(sender_id) + " " + str(req))
        term = req[TERM_KEY]
        is_success = req[SUCCESS]

        if self.role == Role.LEADER:
            if term > self.state.curr_term:
                # step downnnnn from a leader to a follower
                self.role = Role.FOLLOWER
                self.sent_heartbeat_event.set()
            elif not is_success:
                # TODO: log work
                with self.restoration_lock:
                    self.peer_next_log_index[sender_id] -= 1
                    self.restoration_in_progress[sender_id] = True
                peer_last_log_consistent_entry_index = self.peer_next_log_index[sender_id]
                peer_last_log_consistent_entry_term = self.state.log[peer_last_log_consistent_entry_index].term
                entries = self.state.log[peer_last_log_consistent_entry_index+1:]
                self.send_append_entries_to_peer(sender_id, entries, peer_last_log_consistent_entry_index,
                                                 peer_last_log_consistent_entry_term)
            else:
                with self.restoration_lock:
                    self.state.update_response_count_since(self.peer_next_log_index[sender_id])
                    self.peer_next_log_index[sender_id] = self.state.get_last_log_index() + 1
                    self.restoration_in_progress[sender_id] = False


    def handle_append_entry(self, req, sender_id):
        logger.info("APPEND_ENTRY received from " + str(sender_id) + " " + str(req))
        term = req[TERM_KEY]
        leader_id = req[SENDER_ID]
        prev_log_idx = req[PREV_LOG_IDX]
        prev_log_term = req[PREV_LOG_TERM]
        entries = req[ENTRIES]
        commit_idx = req[COMMIT_IDX]
        if self.role == Role.CANDIDATE:
            self.role = Role.FOLLOWER
            self.election_decision_event.set()

        elif self.role == Role.FOLLOWER:
            self.heartbeat_event.set()
        if term < self.state.curr_term:
            self.respond_entry_to_leader(self.state.curr_term, False, leader_id)

        elif term > self.state.curr_term:
            self.state.curr_term = term
            if self.role == Role.LEADER:
                self.role = Role.FOLLOWER
                self.sent_heartbeat_event.set()

        if term == self.state.curr_term:
            # setting the leader id for each appendEntryRPC
            self.leader_id = sender_id
            # TODO: Do log consistency check
            logger.info(
                f"prev_log_idx : {prev_log_idx}, last_log_idx : {self.state.get_last_log_index()}, "
                f"prev_log_term : {prev_log_term}, last_log_term : {self.state.get_last_log_term()}")

            # the case when the follower has more entries in its log than the leader
            if self.state.get_last_log_index() > prev_log_idx:
                self.state.trim_logs_to_index(prev_log_idx)

            if prev_log_idx == self.state.get_last_log_index() \
                    and prev_log_term == self.state.get_last_log_term():
                # TODO : not needed
                # and entries[0].command == self.state.log[self.last_log_idx].command:
                # heartbeat event, so nothing to extend
                if not entries:
                    return

                logger.info("extending logs with entries")
                with self.state.lock:
                    self.state.log = self.state.log[:prev_log_idx + 1] + entries
                logger.info(self.state.log)
                self.respond_entry_to_leader(term, True, sender_id)

            else:
                logger.info("logs are not consistent. Need to go further back in time in the logs")
                self.respond_entry_to_leader(term, False, sender_id)

    def handle_respond_vote(self, req, sender_id):
        logger.info("RESPOND_VOTE recieved from " + str(sender_id) + " " + str(req))
        if self.role == Role.CANDIDATE:
            sender_id = req[SENDER_ID]
            term = req[TERM_KEY]
            vote_granted = req[VOTE_GRANTED]

            if term > self.state.curr_term:
                # step down to follower
                self.role = Role.FOLLOWER
                self.election_decision_event.set()
            elif vote_granted:
                self.votes_recvd += 1
                # TODO: we have peers dict -> we have to add one more key, which is true or false
                if self.votes_recvd == (len(self.peer_conn_dict)) // 2 + 1:
                    self.leader_id = self.client_id
                    self.role = Role.LEADER
                    self.election_decision_event.set()

    def handle_request_vote(self, req, sender_id):
        logger.info("REQUEST_VOTE received from " + str(sender_id) + " " + str(req))
        # some candidate has requested vote from you
        term = req[TERM_KEY]
        last_log_idx = req[LAST_LOG_IDX]
        last_log_term = req[LAST_LOG_TERM]
        if term > self.state.curr_term:
            self.state.curr_term = term
            self.voted_for = None

            if self.role == Role.CANDIDATE:
                logger.info("got another candidate which has term - so i am losing election")
                self.role = Role.FOLLOWER
                self.election_decision_event.set()
            elif self.role == Role.LEADER:
                logger.info("Stepping down as leader to " + sender_id + " who has the term " + str(term))
                self.role = Role.FOLLOWER
                self.sent_heartbeat_event.set()
        if term == self.state.curr_term:

            if self.voted_for is not None:
                self.response_vote_to_candidate(self.state.curr_term, False, sender_id)
            # elif self.last_log_term > last_log_term or (self.last_log_term == last_log_term and self.last_log_idx > last_log_idx):
            #    self.response_vote_to_candidate(self.state.curr_term, False)
            else:
                self.voted_for = sender_id
                self.response_vote_to_candidate(self.state.curr_term, True, sender_id)

    def re_establish_connection(self, sender_id):
        sender_peer_conn = self.peer_conn_dict[sender_id]
        request = {EVENT_KEY: Event.TEST_CONNECTION,
                   SENDER_ID: self.client_id,
                   }
        time.sleep(1)
        try:
            sender_peer_conn.sendall(pickle.dumps(request) + pickle.dumps(Event.EOM))
        except Exception as ex:
            # recreate the socket and reconnect
            logger.error(ex)
            peer_addr = (self.clients_info[sender_id]["host"], self.clients_info[sender_id]["port"])
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            logger.info("Trying to connect to peer : " + sender_id)
            sock.connect(peer_addr)
            logger.info("Now connected to peer : " + sender_id)
            self.peer_conn_dict[sender_id] = sock
            self.peer_next_log_index[sender_id] = self.state.get_last_log_index() + 1

    def respond_entry_to_leader(self, term, success, recv_id):

        request = {EVENT_KEY: Event.RESPOND_ENTRY,
                   SENDER_ID: self.client_id,
                   TERM_KEY: term,
                   SUCCESS: success}

        time.sleep(1)
        logger.info("Sending respond entry")
        self.send_msg_to_peer(recv_id, request)

    def response_vote_to_candidate(self, term, vote_granted, recv_id):

        request = {EVENT_KEY: Event.RESPOND_VOTE,
                   SENDER_ID: self.client_id,
                   TERM_KEY: term,
                   VOTE_GRANTED: vote_granted}

        time.sleep(1)
        logger.info("Sending vote response to client")
        self.send_msg_to_peer(recv_id, request)

    def handle_create_group(self):
        group_id = self.get_new_group_id()
        group_public_key, group_private_key = self.generate_encryption_keys_for_group()
        logger.info("Enter the client ids that you want to add to the group, separated by space : ")
        client_id_list = input(">").strip().split()
        # check to see if all the client ids are valid or not!
        if not all([client_id in self.peer_conn_dict for client_id in client_id_list]):
            logger.warning("Some of the Client ids are not valid. Aborting process!")
            return

        encrypted_group_private_key_dict = self.encrypt_group_private_key(group_private_key, client_id_list)
        # TODO: how to get the current term
        log_entry_request = {
            EVENT_KEY: Event.NEW_GROUP,
            SENDER_ID: self.client_id,
            TERM_KEY: self.state.curr_term,
            GROUP_ID: group_id,
            CLIENT_IDS: client_id_list,
            GROUP_PUBLIC_KEY: group_public_key,
            ENCRYPTED_GROUP_PRIVATE_KEYS_DICT: encrypted_group_private_key_dict
        }
        time.sleep(1)
        self.send_msg_to_peer(self.leader_id, log_entry_request)

    def send_msg_to_peer(self, peer_id, request):
        logger.info(f"Receiver_id : {peer_id} , request : {request}")
        if peer_id == self.client_id:
            conn = self.self_server_conn
        else:
            conn = self.peer_conn_dict[peer_id]
        try:
            conn.sendall(pickle.dumps(request) + pickle.dumps(Event.EOM))
            logger.info(f"Successfully sent msg to peer : {peer_id}")
        except Exception as ex:
            # TODO: start leader election ?! Once new leader available, retry operation!
            logger.error(f"Unable to send msg to peer : {peer_id}")
            logger.error(ex)
            # TODO :lets remove the conn from the dict if it fails ?
            # del self.peer_conn_dict[peer_id]

    def get_new_group_id(self):
        self.group_counter += 1
        group_id = self.client_id + "_" + self.group_counter
        return group_id

    @staticmethod
    def generate_encryption_keys_for_group():
        return rsa.newkeys(16)

    def encrypt_group_private_key(self, group_private_key, client_id_list):
        client_id_group_private_key_dict = dict()
        for peer_id in client_id_list:
            client_id_group_private_key_dict[peer_id] = rsa.encrypt(bytes(group_private_key),
                                                                    self.public_keys_dict[peer_id])
        return client_id_group_private_key_dict

    def send_new_command_to_leader(self):
        log_entry_request = {
            EVENT_KEY: Event.NEW_COMMAND,
            SENDER_ID: self.client_id,
            COMMAND: "new command",
            REQUEST_ID: self.client_id + "_" + str(self.state.increment_and_get_client_counter())
        }
        while True:
            try:
                logger.info("my leader is : " + str(self.leader_id))
                self.send_msg_to_peer(self.leader_id, log_entry_request)
                logger.info(f"New command entries sent to leader {self.leader_id} : " + str(log_entry_request))
                break
            except Exception as ex:
                logger.warning(
                    "Failed to send message to leader : " + str(self.leader_id) + ". Trying again in sometime...")
                logger.error(ex)
                # timeout for retry
                time.sleep(3)

    def initialize_peer_next_log_index(self):
        # TODO :is it thread safe / should we move this to the state class
        next_log_index = self.state.get_last_log_index()
        with self.restoration_lock:
            for peer_id in self.peer_conn_dict:
                self.peer_next_log_index[peer_id] = next_log_index + 1
                self.restoration_in_progress[peer_id] = False
                self.state.client_req_respond_count.clear()


    def connect_to_self_server(self):
        peer_addr = (self.clients_info[self.client_id]["host"], self.clients_info[self.client_id]["port"])
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        logger.info("Trying to connect to self server..")
        try:
            sock.connect(peer_addr)
            logger.info("Now connected to self server. Yay!")
            self.self_server_conn = sock
        except Exception as ex:
            logger.error(f"Could not CONNECT to self server!")
            logger.error(ex)


if __name__ == '__main__':
    client_id = sys.argv[1]
    with open(os.path.join(pathlib.Path(__file__).parent.resolve(), 'config.json'), 'r') as config_file:
        config_info = json.load(config_file)
        clients_info = config_info["clients"]
        if client_id not in clients_info:
            logger.error("Invalid client id. Please check...")
        else:
            logger.info("Initiating client..")
            Client(client_id, clients_info)
