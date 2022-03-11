import selectors
import json
import logging
import pathlib
import selectors
import socket
import sys
import time
import types
import pickle
import rsa

from client_menu import Client_Menu
from util_classes import *

logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)

logger = logging.getLogger(__name__)


class Client:
    EOM_IN_BYTES = pickle.dumps(Event.EOM)

    def __init__(self, client_id, clients_info):

        self.client_menu = Client_Menu()

        # to store all the groups the client is a part of
        self.groups_dict = dict()

        # config file state
        self.peer_next_log_index = dict()
        self.restoration_in_progress = dict()
        self.self_server_conn = None
        self.client_id = client_id
        self.clients_info = clients_info
        self.restoration_lock = threading.Lock()
        self.majority = (len(self.clients_info)) // 2 + 1

        # application related state
        self.private_key, self.public_keys_dict = self.load_keys(self.client_id)

        # stores conn objs of all peers
        self.peer_conn_dict = dict()

        # load state from file if existed before
        self.state_file = 'raft-state-%s' % self.client_id
        self.persisted_state_path = os.path.join(State.save_dir, f'raft-state-{self.client_id}.pickle')
        self.load_state()
        self.leader_id = None
        self.votes_recvd = 0
        self.role = Role.FOLLOWER

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
        self.state = None
        try:
            with open(self.persisted_state_path, 'rb') as f:
                self.state = pickle.load(f)
                logger.info("state was retrieved successfully!")
                # act on the entries once the process comes back up
                # self.act_on_commited_entries(1, self.state.commit_idx)
        except Exception as ex:
            self.state = State(0, None, [LogEntry(0, 0, {LOG_ENTRY_TYPE: Log_entry_type.DUMMY})])
            logger.warning("No restoration file found!")
            logger.error(ex)

    def save_state(self):
        with self.state.lock:
            with open(self.persisted_state_path, 'wb') as handle:
                pickle.dump(self.state, handle)
                logger.info("State was successfully persisted!")

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
            logger.debug("hearbeat recvd")
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
        self.send_heartbeats_to_peers()

        while self.role == Role.LEADER:
            # TODO:  fix the timeout value
            flag = self.sent_heartbeat_event.wait(timeout=5)
            # if has to step down during the timeout
            if self.role != Role.LEADER:
                break

            if flag:
                logger.info("someone sent append rpc - restart the sending of heartbeat")
            else:
                logger.debug("time to send heartbeats")
                self.send_heartbeats_to_peers()

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

    def send_heartbeats_to_peers(self):
        with self.state.lock:
            request = {EVENT_KEY: Event.APPEND_ENTRY,
                       SENDER_ID: self.client_id,
                       TERM_KEY: self.state.curr_term,
                       PREV_LOG_IDX: self.state.get_last_log_index(),
                       PREV_LOG_TERM: self.state.get_last_log_term(),
                       ENTRIES: [],
                       COMMIT_IDX: self.state.commit_idx}

        time.sleep(1)

        for peer_id in self.peer_conn_dict:
            logger.debug(f"Sending append entries to client.")
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
            self.client_menu.display_menu()
            # first ever command implemented
            # self.send_new_command_to_leader()
            # logger.info(" - createGroup <client_ids>")
            # logger.info(" - add <group id> <client id>")
            # logger.info(" - kick <group id> <client id>")
            # logger.info(" - writeMessage <group id> <message>")
            # logger.info(" - printGroup <group id>")
            # logger.info(" - failLink <src> <dest>")
            # logger.info(" - fixLink <src> <dest>")
            # logger.info(" - failProcess")
            logger.info("Client prompt >> ")
            user_input = input().strip().split()

            if not user_input:
                continue
            action = user_input[0]
            # load state before every action
            self.act_on_commited_entries(1, self.state.commit_idx)
            if action == "createGroup":
                self.handle_create_group(user_input[1:])
            elif action == "add":
                self.handle_add_member(user_input[1:])
            elif action == "kick":
                self.handle_kick_member(user_input[1:])
            elif action == "writeMessage":
                self.handle_write_message(user_input[1:])
            elif action == "printGroup":
                self.handle_print_group(user_input[1:])
            elif action == "failLink":
                self.handle_fail_link(user_input[1:])
            elif action == "fixLink":
                self.handle_fix_link(user_input[1:])
            elif action == "failProcess" or action == '4':
                self.server_flag.clear()
                logger.info("Until next time...")
                break
            elif action == "9":
                # TODO: just testting append RPC for now
                logger.info("here are the logs : ")
                logger.info(self.state.log)
            else:
                logger.warning("Incorrect menu option. Please try again..")
            # clear state after every action
            self.groups_dict.clear()

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
                self.peer_conn_dict[other_client_id] = [True, sock]
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

        if self.peer_conn_dict[sender_id][0]:
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

            elif req[EVENT_KEY] == Event.NEW_CONNECTION:
                self.re_establish_connection(sender_id)

            # elif req[EVENT_KEY] == Event.NEW_COMMAND:
            #     self.handle_new_command(req, sender_id)
            #
            # elif req[EVENT_KEY] == Event.FAIL_LINK:
            #     self.handle_fail_link_command(sender_id)
            #
            # elif req[EVENT_KEY] == Event.FIX_LINK:
            #     self.handle_fix_link_command(sender_id)

            elif req[EVENT_KEY] == Event.COMMAND_SUCCESS_NOTIFICATION:
                self.handle_success_notification(req)

            elif req[EVENT_KEY] == Event.CLIENT_REQUEST:
                self.handle_client_request(req, sender_id)
        else:
            logger.info("broken link cannot process message")

    # TODO : delete this
    def handle_new_command(self, req, sender_id):
        logger.info("NEW_COMMAND received from " + str(sender_id) + " " + str(req))
        command = req[COMMAND]
        request_id = command[REQUEST_ID]

        if self.role == Role.LEADER:
            with self.state.lock:
                # if self.state.req_in_logs(request_id):
                #     return
                logger.info("handle new command lock acquired")
                command = {REQUEST_ID: request_id, 'MESSAGE': command}
                self.state.append_to_log(request_id, command)
                # we cant maintain count as the same peer can vote twice, in case he goes down and comes back up
                self.state.client_req_respond_count[request_id] = {self.client_id}
            # no need to send append entry, will be taken care by the heartbeat thread.
            # self.send_append_entries_to_peers(last_log_index=last_log_idx, last_log_term=last_log_term,
            #                                   entries=[log_entry])

    def handle_respond_entry(self, req, sender_id):
        logger.debug("RESPOND_ENTRY received from " + str(sender_id) + " " + str(req))
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
                entries = self.state.log[peer_last_log_consistent_entry_index + 1:]
                self.send_append_entries_to_peer(sender_id, entries, peer_last_log_consistent_entry_index,
                                                 peer_last_log_consistent_entry_term)
            else:
                with self.restoration_lock:
                    prev_commit_idx = self.state.commit_idx
                    commit_idx_updated = self.state.update_response_count_since(self.peer_next_log_index[sender_id],
                                                                                self.majority, sender_id)
                    self.peer_next_log_index[sender_id] = self.state.get_last_log_index() + 1
                    self.restoration_in_progress[sender_id] = False
                    if commit_idx_updated:
                        self.save_state()
                        # self.act_on_commited_entries(prev_commit_idx + 1, self.state.commit_idx)
                        self.respond_client_with_success(prev_commit_idx + 1, self.state.commit_idx)

    def handle_append_entry(self, req, sender_id):
        logger.debug("APPEND_ENTRY received from " + str(sender_id) + " " + str(req))
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
            logger.debug(
                f"leader_log_idx : {prev_log_idx}, last_log_idx : {self.state.get_last_log_index()}, "
                f"leader_log_term : {prev_log_term}, last_log_term : {self.state.get_last_log_term()},"
                f"leader_commit_idx : {commit_idx} , last_commit_idx : {self.state.commit_idx}")

            # the case when the follower has more entries in its log than the leader
            if self.state.get_last_log_index() > prev_log_idx:
                self.state.trim_logs_to_index(prev_log_idx)

            if prev_log_idx == self.state.get_last_log_index() \
                    and prev_log_term == self.state.get_last_log_term():
                # TODO : not needed
                # and entries[0].command == self.state.log[self.last_log_idx].command:
                # heartbeat event, so nothing to extend
                if entries:
                    logger.debug("extending logs with entries")
                    with self.state.lock:
                        self.state.log = self.state.log[:prev_log_idx + 1] + entries
                    logger.debug(self.state.log)
                if commit_idx > self.state.commit_idx:
                    prev_commit_idx = self.state.commit_idx
                    self.state.update_commit_idx(commit_idx)
                    # self.act_on_commited_entries(prev_commit_idx + 1, self.state.commit_idx)
                    self.save_state()
                    logger.info(f"COMMIT idx updated to {self.state.commit_idx}")
                self.respond_entry_to_leader(term, True, sender_id)


            else:
                logger.debug("logs are not consistent. Need to go further back in time in the logs")
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
            elif self.state.get_last_log_term() > last_log_term or (self.state.get_last_log_term() == last_log_term and
                                                                    self.state.get_last_log_index() > last_log_idx):
               self.response_vote_to_candidate(self.state.curr_term, False, sender_id)
            else:
                self.voted_for = sender_id
                self.response_vote_to_candidate(self.state.curr_term, True, sender_id)

    def re_establish_connection(self, sender_id):
        sender_peer_conn = self.peer_conn_dict[sender_id][1]
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
            self.peer_conn_dict[sender_id] = [True, sock]
            self.peer_next_log_index[sender_id] = self.state.get_last_log_index() + 1

    def respond_entry_to_leader(self, term, success, recv_id):

        request = {EVENT_KEY: Event.RESPOND_ENTRY,
                   SENDER_ID: self.client_id,
                   TERM_KEY: term,
                   SUCCESS: success}

        time.sleep(1)
        logger.debug("Sending respond entry")
        self.send_msg_to_peer(recv_id, request)

    def response_vote_to_candidate(self, term, vote_granted, recv_id):

        request = {EVENT_KEY: Event.RESPOND_VOTE,
                   SENDER_ID: self.client_id,
                   TERM_KEY: term,
                   VOTE_GRANTED: vote_granted}

        time.sleep(1)
        logger.info("Sending vote response to client")
        self.send_msg_to_peer(recv_id, request)

    def send_msg_to_peer(self, peer_id, request):
        if self.peer_conn_dict[peer_id][0]:
            logger.debug(f"Receiver_id : {peer_id} , request : {request}")
            if peer_id == self.client_id:
                conn = self.self_server_conn
            else:
                conn = self.peer_conn_dict[peer_id][1]
            try:
                conn.sendall(pickle.dumps(request) + pickle.dumps(Event.EOM))
                logger.debug(f"Successfully sent msg to peer : {peer_id}")
            except Exception as ex:
                # TODO: start leader election ?! Once new leader available, retry operation!
                logger.error(f"Unable to send msg to peer : {peer_id}")
                logger.error(ex)
                # DONE :lets remove the conn from the dict if it fails  -> we are making it a tuplem now 
        else:
            logger.info("cannot send message as link is broken")
            
    def get_new_group_id(self):
        self.state.group_counter += 1
        group_id = self.client_id + "_" + str(self.state.group_counter)
        return group_id

    @staticmethod
    def generate_encryption_keys_for_group():
        return rsa.newkeys(512)

    @staticmethod
    def get_string_from_private_key(private_key):
        return "-".join(
            [str(i) for i in [private_key.n, private_key.e, private_key.d, private_key.p, private_key.q]]).encode(
            'utf8')

    @staticmethod
    def get_private_key_from_string(private_key_string):
        splitted_list = [int(i) for i in private_key_string.decode('utf8').split('-')]
        return rsa.key.PrivateKey(*splitted_list)

    def encrypt_group_private_key(self, group_private_key, client_id_list):
        client_id_group_private_key_dict = dict()
        for peer_id in client_id_list:
            client_id_group_private_key_dict[peer_id] = rsa.encrypt(self.get_string_from_private_key(group_private_key),
                                                                    self.public_keys_dict[peer_id])
        return client_id_group_private_key_dict

    def send_new_command_to_leader(self):
        log_entry_request = {
            EVENT_KEY: Event.NEW_COMMAND,
            SENDER_ID: self.client_id,
            COMMAND: "new command",
            REQUEST_ID: self.generate_request_id()
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

    def get_client_log_entry_request(self, command):
        log_entry_request = {
            EVENT_KEY: Event.CLIENT_REQUEST,
            SENDER_ID: self.client_id,
            TERM_KEY: self.state.curr_term,
            COMMAND: command
        }
        return log_entry_request

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

    def respond_client_with_success(self, commit_start_idx, commit_end_idx):
        for log_entry in self.state.log[commit_start_idx: commit_end_idx + 1]:
            request_id = log_entry.command[REQUEST_ID]

            initiator_id = request_id.split('_')[0]

            request = {EVENT_KEY: Event.COMMAND_SUCCESS_NOTIFICATION,
                       SENDER_ID: self.client_id,
                       TERM_KEY: self.state.curr_term,
                       REQUEST_ID: request_id}

            # TODO: add timer
            # time.sleep(1)
            logger.info("Sending command commit response to client")
            self.send_msg_to_peer(initiator_id, request)

    def handle_success_notification(self, req):
        logger.info(f"Yo! The command with the following request id was committed successfully! : {req[REQUEST_ID]} "
                    f": by leader : {req[SENDER_ID]}")

    def handle_client_request(self, req, sender_id):
        logger.info("NEW GROUP EVENT received from " + str(sender_id) + " " + str(req))
        command = req[COMMAND]
        request_id = command[REQUEST_ID]

        if self.role == Role.LEADER:
            with self.state.lock:
                # this is only needed if client retry is implemented
                # if self.state.req_in_logs(request_id):
                #     return
                self.state.append_to_log(command)
                # we cant maintain count as the same peer can vote twice, in case he goes down and comes back up
                self.state.client_req_respond_count[request_id] = {self.client_id}
        else:
            # redirect the message to the leader
            self.send_msg_to_peer(self.leader_id, req)

    def generate_request_id(self):
        return self.client_id + "_" + str(self.state.increment_and_get_client_counter())

    def act_on_commited_entries(self, prev_commit_idx, commit_idx):
        for log_entry in self.state.log[prev_commit_idx:commit_idx + 1]:
            command = log_entry.command
            if command[LOG_ENTRY_TYPE] == Log_entry_type.CREATE_ENTRY:
                self.handle_create_entry(command)
            elif command[LOG_ENTRY_TYPE] == Log_entry_type.ADD_ENTRY:
                self.handle_add_entry(command)
            elif command[LOG_ENTRY_TYPE] == Log_entry_type.KICK_ENTRY:
                self.handle_kick_entry(command)
            elif command[LOG_ENTRY_TYPE] == Log_entry_type.MESSAGE_ENTRY:
                self.handle_message_entry(command)
            else:
                logger.warning(f"This action cannot be handled : {command[ACTION]}")

    def handle_create_entry(self, command):
        # always return a new list, so that the log entry remains consistent
        client_id_list = list(command[CLIENT_IDS])
        if self.client_id not in client_id_list:
            logger.debug("I am not a part of this new group. sed")
            return
        group_id = command[GROUP_ID]
        group_public_key = command[GROUP_PUBLIC_KEY]
        group_private_key = self.get_private_key_from_string(
            rsa.decrypt(command[ENCRYPTED_GROUP_PRIVATE_KEYS_DICT][self.client_id], self.private_key))
        new_group = Group(group_id, group_public_key, group_private_key, client_id_list)
        self.groups_dict[group_id] = new_group
        logger.debug(f"successfully added self to new group with id : {group_id}")

    def handle_add_entry(self, command):
        group_id = command[GROUP_ID]
        client_id_to_add = command[CLIENT_ID]

        if group_id not in self.groups_dict and client_id_to_add != self.client_id:
            logger.debug("This add entry doesn't concern me. ")
        elif client_id_to_add != self.client_id:
            self.groups_dict[group_id].client_id_list.append(client_id_to_add)
            logger.debug(f"Added new member {client_id_to_add} to group : {group_id}")
        else:
            group_private_key = self.get_private_key_from_string(
                rsa.decrypt(command[ENCRYPTED_GROUP_PRIVATE_KEY], self.private_key))
            group_public_key = None
            client_id_list = []
            # figure out the membership of the group from the logs
            for log_entry in self.state.log[1:]:
                log_entry_command = log_entry.command
                if log_entry_command[LOG_ENTRY_TYPE] == Log_entry_type.CREATE_ENTRY \
                        and log_entry_command[GROUP_ID] == group_id:
                    client_id_list = list(log_entry_command[CLIENT_IDS])
                    group_public_key = log_entry_command[GROUP_PUBLIC_KEY]
                elif log_entry_command[LOG_ENTRY_TYPE] == Log_entry_type.ADD_ENTRY \
                        and log_entry_command[GROUP_ID] == group_id:
                    client_id_list.append(command[CLIENT_ID])
                elif log_entry_command[LOG_ENTRY_TYPE] == Log_entry_type.KICK_ENTRY \
                        and log_entry_command[GROUP_ID] == group_id:
                    client_id_list.remove(command[CLIENT_ID])
                    group_public_key = log_entry_command[GROUP_PUBLIC_KEY]

            new_group = Group(group_id, group_public_key, group_private_key, client_id_list)
            self.groups_dict[group_id] = new_group
            logger.debug(f"successfully added self to new group with id : {group_id}")

    def handle_kick_entry(self, command):
        group_id = command[GROUP_ID]
        client_id_to_kick = command[CLIENT_ID]
        if group_id not in self.groups_dict:
            logger.debug("This kick entry doesn't concern me.")
        elif client_id_to_kick != self.client_id:
            group = self.groups_dict[group_id]
            group.client_id_list.remove(client_id_to_kick)
            logger.debug(f"Kicked member {client_id_to_kick} from group : {group_id}")
            group_public_key = command[GROUP_PUBLIC_KEY]
            group_private_key = self.get_private_key_from_string(
                rsa.decrypt(command[ENCRYPTED_GROUP_PRIVATE_KEYS_DICT][self.client_id], self.private_key))
            group.public_key = group_public_key
            group.private_key = group_private_key
        else:
            # just get out the client list, you still maintain the messages.
            group = self.groups_dict[group_id]
            group.client_id_list.remove(client_id_to_kick)

    def handle_message_entry(self, command):
        group_id = command[GROUP_ID]
        sender_id = command[SENDER_ID]
        encrypted_message = command[ENCRYPTED_MESSAGE]

        if group_id not in self.groups_dict:
            logger.debug("this message does not concern me as i was never part of this group")
        elif self.client_id not in self.groups_dict[group_id].client_id_list:
            logger.debug("this message does not concern me as i am kicked out from this group")
        else:
            group = self.groups_dict[group_id]
            message = rsa.decrypt(encrypted_message, group.private_key).decode('utf8')
            group.messages.append(" : ".join([sender_id, message]))

            logger.debug(f"Added message {message} from sender {sender_id} in group {group_id}")

    def handle_create_group(self, client_ids):
        group_id = self.get_new_group_id()
        group_public_key, group_private_key = self.generate_encryption_keys_for_group()
        # logger.info("Enter the client ids that you want to add to the group, separated by space : ")
        client_id_list = client_ids

        # check to see if all the client ids are valid or not!
        if not all([client_id in self.peer_conn_dict or client_id == self.client_id for client_id in client_id_list]):
            logger.warning("Some of the Client ids are not valid. Aborting process!")
            return
        # add self
        client_id_list.append(self.client_id)
        # remove duplicates
        client_id_list = list(set(client_id_list))

        encrypted_group_private_key_dict = self.encrypt_group_private_key(group_private_key, client_id_list)
        command = {
            LOG_ENTRY_TYPE: Log_entry_type.CREATE_ENTRY,
            REQUEST_ID: self.generate_request_id(),
            GROUP_ID: group_id,
            CLIENT_IDS: client_id_list,
            GROUP_PUBLIC_KEY: group_public_key,
            ENCRYPTED_GROUP_PRIVATE_KEYS_DICT: encrypted_group_private_key_dict
        }
        # TODO: how to get the current term
        log_entry_request = self.get_client_log_entry_request(command)
        time.sleep(1)
        self.send_msg_to_peer(self.leader_id, log_entry_request)

    def handle_kick_member(self, param):
        group_id = param[0]
        client_id_to_kick = param[1]
        # if group_id not in self.groups_dict:
        #     logger.warning("Oops! Cannot kick a member from an alien group..")
        #
        # elif client_id_to_kick not in self.peer_conn_dict or client_id != self.client_id:
        #     logger.warning("Invalid client it. Aborting..")
        #
        # elif client_id_to_kick not in self.groups_dict[group_id].client_id_list:
        #     logger.info("Client already absent from this group..")

        # else:
        group_public_key, group_private_key = self.generate_encryption_keys_for_group()
        client_id_list = list(self.groups_dict[group_id].client_id_list)
        client_id_list.remove(client_id_to_kick)
        encrypted_group_private_key_dict = self.encrypt_group_private_key(group_private_key, client_id_list)

        command = {
            LOG_ENTRY_TYPE: Log_entry_type.KICK_ENTRY,
            REQUEST_ID: self.generate_request_id(),
            GROUP_ID: group_id,
            CLIENT_ID: client_id_to_kick,
            GROUP_PUBLIC_KEY: group_public_key,
            ENCRYPTED_GROUP_PRIVATE_KEYS_DICT: encrypted_group_private_key_dict
        }
        time.sleep(1)
        log_entry_request = self.get_client_log_entry_request(command)
        self.send_msg_to_peer(self.leader_id, log_entry_request)

    def handle_add_member(self, param):
        group_id = param[0]
        client_id_to_add = param[1]
        # if group_id not in self.groups_dict:
        #     logger.warning("Oops! Cannot add a member to an alien group..")
        #
        # elif client_id_to_add not in self.peer_conn_dict:
        #     logger.warning("Invalid client it. Aborting..")
        #
        # elif client_id_to_add in self.groups_dict[group_id].client_id_list:
        #     logger.info("Client already added to this group..")

        # else:
        group_private_key_encrypted = rsa.encrypt(
            self.get_string_from_private_key(self.groups_dict[group_id].private_key),
            self.public_keys_dict[client_id_to_add])
        command = {
            LOG_ENTRY_TYPE: Log_entry_type.ADD_ENTRY,
            REQUEST_ID: self.generate_request_id(),
            GROUP_ID: group_id,
            CLIENT_ID: client_id_to_add,
            ENCRYPTED_GROUP_PRIVATE_KEY: group_private_key_encrypted
        }
        # TODO: how to get the current term
        log_entry_request = self.get_client_log_entry_request(command)
        time.sleep(1)
        self.send_msg_to_peer(self.leader_id, log_entry_request)

    def handle_write_message(self, param):

        group_id = param[0]
        if len(param) < 2:
            logger.error("No message found. Try again..")
        message = " ".join(param[1:]).encode('utf8')

        message_encrypted = rsa.encrypt(message, self.groups_dict[group_id].public_key)

        command = {
            LOG_ENTRY_TYPE: Log_entry_type.MESSAGE_ENTRY,
            REQUEST_ID: self.generate_request_id(),
            GROUP_ID: group_id,
            SENDER_ID: self.client_id,
            ENCRYPTED_MESSAGE: message_encrypted,
        }

        log_entry_request = self.get_client_log_entry_request(command)
        time.sleep(1)
        self.send_msg_to_peer(self.leader_id, log_entry_request)
    
    def handle_fix_link(self, param):
        src_id = self.client_id
        dest_id = param[0]
        if src_id == dest_id:
            logger.info("Same destination link cannot be fixed")

        elif dest_id not in self.peer_conn_dict:
            logger.warning("Invalid connection id. Try again!")
        else:
            request = {
                EVENT_KEY: Event.FIX_LINK,
                SENDER_ID: self.client_id
            }
            # logger.info(f"Send FIX LINK event to peer : {dest_id}")
            # self.send_msg_to_peer(dest_id, request)
            self.peer_conn_dict[dest_id][0] = False
            logger.info(f"FIXED LINK with peer : {dest_id}")
        
    def handle_fail_link(self, param):
        src_id = self.client_id
        dest_id = param[0]

        if src_id == dest_id:
            logger.warning("Same destination link cannot be removed")

        elif dest_id not in self.peer_conn_dict:
            logger.warning("Invalid connection id. Try again!")
        else:
            request = {
                EVENT_KEY: Event.FAIL_LINK,
                SENDER_ID: self.client_id
            }
            # logger.info(f"Send FAIL LINK event to peer : {dest_id}")
            # self.send_msg_to_peer(dest_id, request)
            self.peer_conn_dict[dest_id][0] = False
            logger.info(f"FAILED LINK with peer : {dest_id}")


    def handle_print_group(self, param):
        group_id = param[0]
        # logger.info("Following are the groups I am a part of : ")
        # logger.info(f"Total no of groups : {len(self.groups_dict)}")
        if group_id not in self.groups_dict:
            logger.info(f"No message for you from group : {group_id}. Sed")
        else:
            if not self.groups_dict[group_id].messages:
                logger.info(f"No message for you in group : {group_id}")
            else:
                logger.info(f"Here are your messages from group : {group_id}")
                all_messages = "\n" + "\n".join(self.groups_dict[group_id].messages)
                logger.info(all_messages)

    def handle_fail_link_command(self, sender_id):
        self.peer_conn_dict[sender_id][0] = False
        logger.info(f"FAILED LINK with peer : {sender_id}")

    def handle_fix_link_command(self, sender_id):
        self.peer_conn_dict[sender_id][0] = True
        logger.info(f"FIXED LINK with peer : {sender_id}")


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
