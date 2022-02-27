from http import client
import pickle
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


logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)

KEYOBJECT_FILE_PATH = "keystore.obj"
TERM_KEY = "curr_term"
EVENT_KEY = "type"
SENDER_ID = "sender_id"
LAST_LOG_IDX = "last_log_idx"
LAST_LOG_TERM = "last_log_term"
ENTRIES = "entries"
PREV_LOG_IDX = "prev_log_idx"
PREV_LOG_TERM = "prev_log_term"
COMMIT_IDX = "commit_idx"
VOTE_GRANTED = "voted_granted"
SUCCESS = "SUCCESS"

class Role(enum.Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

class State:

    def __init__(self, curr_term, voted_for, log):
        self.curr_term = curr_term
        self.voted_for = voted_for
        self.log = log


class Event(enum.Enum):
    REQUEST_VOTE = 1
    RESPOND_VOTE = 2
    APPEND_ENTRY = 3
    RESPOND_ENTRY = 4
    EOM = 5

class Client:

    EOM_IN_BYTES = pickle.dumps(Event.EOM)

    def __init__(self, client_id, clients_info):

        # config file state
        self.client_id = client_id
        self.clients_info = clients_info


        #application related state
        self.public_keys, self.private_key = self.load_keys(self.client_id)

        # stores conn objs of all peers
        self.peers = dict()

        #load state from file if existed before
        self.state_file = 'raft-state-%s' % self.client_id
        self.load_state()
        self.last_log_idx = self.state.log[-1] if self.state.log else -1
        self.last_log_term = self.state.log[-1] if self.state.log else -1
        self.prev_log_idx = 0
        self.prev_log_term = 0
        self.commit_idx = 0
        self.entries = list()
        self.leader_id = None
        self.votes_recvd = 0

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
        
        time.sleep(10)
        logger.info("starting leader election now!")
        # input("PRESS ANY KEY TO START LEADER ELECTION!!")
        self.role = Role.FOLLOWER

        state_thread = threading.Thread(target=self.start_state_thread)
        state_thread.daemon = True
        state_thread.start()

        time.sleep(5)
        self.start_ui()

    def load_state(self):
        self.state = dict()
        try:
            with open(self.state_file, 'r') as f:
                self.state = pickle.load(f)
        except Exception as e:
            self.state = State(0, None, list())

    def load_keys(self, client_idx):
        with open(KEYOBJECT_FILE_PATH, 'rb') as keystore:
            keys = pickle.load(keystore)
        return (keys[int(client_idx)-1], [key[0] for key in keys])
    
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
            recv_data = sock.recv(1024)
            
            if recv_data:
                req_list = self.get_request_list(recv_data)
                for req in req_list:
                    self.handle_request_from_peer(req)
            else:
                logger.info('closing connection to : '+ str(data.addr))
                selector.unregister(sock)
                sock.close()
    
    def on_follower(self):
        # can go to candiate with timeout (otherwise will always be follower)

        while self.heartbeat_event.wait(timeout = self.heartbeat_timeout):
            logger.info("hearbeat recvd")
            self.heartbeat_event.clear()
            #repeat the waiting for heartbeat
        
        logger.info("becoming candidate as no heartbeat recvd")
        self.role = Role.CANDIDATE
    
    def on_candidate(self):
        #can only come from follower timeout
        
        
        #send request vote RPC to all peers
        self.request_vote_from_peers()

        while self.election_decision_event.wait(timeout = self.election_timeout) is False:
            logger.info("election decision not recieved")
            self.request_vote_from_peers()
        
        self.election_decision_event.clear()
        #the decision event itself would have set the role now
    
    def on_leader(self):
        logger.info("\n\n\n\nGUIESSS WHO IS LEADER BITCHESSSSSS!!!!!\n\n\n")
        #send appendrpc to all peers in heartbeat time
        self.append_entries_to_peers(heartbeat = True)
        while self.role == Role.LEADER: 
            # TODO:  fix the timeout value
            flag = self.sent_heartbeat_event.wait(timeout = 5)
            #if has to step down during the timeout
            if self.role != Role.LEADER:
                break

            if flag: 
                logger.info("someone sent append rpc - restart the sending of heartbeat")
            else:
                logger.info("time to send heartbeats")
                self.append_entries_to_peers(heartbeat = True)

            self.sent_heartbeat_event.clear()

        
        logger.info("the leader has to step down")
        #the step down event only would have set the role now

    def append_entries_to_peers(self, heartbeat = False):
        
        request =  {EVENT_KEY: Event.APPEND_ENTRY,
            SENDER_ID: self.client_id,
            TERM_KEY: self.state.curr_term,
            PREV_LOG_IDX: self.prev_log_idx,
            PREV_LOG_TERM: self.prev_log_term,
            ENTRIES: [] if heartbeat else self.entries,
            COMMIT_IDX: self.commit_idx}
        
        time.sleep(1)

        for peer_id in self.peers:
            conn = self.peers[peer_id]
            try:
                conn.sendall(pickle.dumps(request) + pickle.dumps(Event.EOM))
                logger.info(f"Append entries sent to client {peer_id} : " + str(request))
            except Exception as ex:
                logger.warning("Failed to send message to client : " + str(peer_id)) 

    def request_vote_from_peers(self):
        self.votes_recvd = 1
        self.state.curr_term += 1
        self.voted_for = self.client_id
        request =  {EVENT_KEY: Event.REQUEST_VOTE,
                    SENDER_ID: self.client_id,
                    TERM_KEY: self.state.curr_term,
                    LAST_LOG_IDX: self.last_log_idx,
                    LAST_LOG_TERM: self.last_log_term}

        time.sleep(1)

        for peer_id in self.peers:
            conn = self.peers[peer_id]

            conn.sendall(pickle.dumps(request) + pickle.dumps(Event.EOM))
            logger.info(f"Vote request sent to client {peer_id} : " + str(request))


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
            user_input = input("Client prompt >> ").strip()
            if user_input == "1":
                receiver_id = input("Enter receiver client id  >> ").strip()
                amount = float(input("Enter the amount in $$ to be transferred to the above client  >> ").strip())
                # tell the transaction thread by adding the event into transaction queue
                self.transaction_thread_queue.put(
                    {"type": "transaction", "receiver_id": receiver_id, "amount": amount, "user_req_id": user_req_id})
            elif user_input == "2":
                # tell the transaction thread by adding the event into the transaction queue
                self.transaction_thread_queue.put({"type": "balance"})
            elif user_input == "3":
                # tell the snapshot thread by adding the event into the snapshot queue
                self.snapshot_thread_queue.put({"type": Event.INIT_SNAPSHOT, "balance":self.balance})
            elif user_input == "4":
                self.server_flag.clear()
                logger.info("Until next time...")
                break
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
            sock.connect(peer_addr)
            logger.info("Now connected to peer : " + other_client_id)
            self.peers[other_client_id] = sock

    def handle_request_from_peer(self, req):

        sender_id = req[SENDER_ID]

        if req[EVENT_KEY] == Event.REQUEST_VOTE:
            logger.info("REQUEST_VOTE recieved from " + str(sender_id) + " " + str(req))

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
                #elif self.last_log_term > last_log_term or (self.last_log_term == last_log_term and self.last_log_idx > last_log_idx):
                #    self.response_vote_to_candidate(self.state.curr_term, False)
                else:
                    self.voted_for = sender_id
                    self.response_vote_to_candidate(self.state.curr_term, True, sender_id)

        elif req[EVENT_KEY] == Event.RESPOND_VOTE:
            logger.info("RESPOND_VOTE recieved from " + str(sender_id) + " " + str(req))

            if self.role == Role.CANDIDATE:
                sender_id = req[SENDER_ID]
                term = req[TERM_KEY]
                vote_granted = req[VOTE_GRANTED]
                

                if term > self.state.curr_term:
                    #step down to follower
                    self.role = Role.FOLLOWER
                    self.election_decision_event.set()
                elif vote_granted:
                    self.votes_recvd += 1
                    #TODO: we have peers dict -> we have to add one more key, which is true or false
                    if self.votes_recvd == (len(self.peers))//2 + 1:
                        self.leader_id = self.client_id
                        self.role = Role.LEADER
                        self.election_decision_event.set()
            
        elif req[EVENT_KEY] == Event.APPEND_ENTRY:
            logger.info("APPEND_ENTRY recieved from " + str(sender_id) + " " + str(req))

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
                self.respond_entry_to_leader(term, False, leader_id)
            elif term > self.state.curr_term:
                self.state.curr_term = term
                if self.role == Role.LEADER:
                    self.role = Role.FOLLOWER
                    self.sent_heartbeat_event.set()
            
            if term == self.state.curr_term:
                #TODO; Do log consistency check
                self.respond_entry_to_leader(term, True, sender_id)
            
        elif req[EVENT_KEY] == Event.RESPOND_ENTRY:
            logger.info("RESPOND_ENTRY recieved from " + str(sender_id) + " " + str(req))

            term = req[TERM_KEY]
            if self.role == Role.LEADER:
                if term > self.state.curr_term:
                    #step downnnnn from a leader to a follower
                    self.role = Role.FOLLOWER
                    self.sent_heartbeat_event.set()
                else:
                    #TODO: log work 
                    pass

    def respond_entry_to_leader(self, term, success, recv_id):

        request =  {EVENT_KEY: Event.RESPOND_ENTRY,
                    SENDER_ID: self.client_id,
                    TERM_KEY: term,
                    SUCCESS: success}

        time.sleep(1)

        conn = self.peers[recv_id]
        conn.sendall(pickle.dumps(request) + pickle.dumps(Event.EOM))
        logger.info(f"Respond entry send to {recv_id} from {self.client_id}: " + str(request))
    
    def response_vote_to_candidate(self, term, vote_granted, recv_id):

        request =  {EVENT_KEY: Event.RESPOND_VOTE,
                    SENDER_ID: self.client_id,
                    TERM_KEY: term,
                    VOTE_GRANTED: vote_granted}

        time.sleep(1)

        conn = self.peers[recv_id]
        conn.sendall(pickle.dumps(request) + pickle.dumps(Event.EOM))
        logger.info(f"Vote response sent to client {recv_id} from {self.client_id} : " + str(request))

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
