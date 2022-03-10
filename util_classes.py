import enum
import pickle
import threading
from typing import List
import os

from constants import *


class Role(enum.Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class LogEntry:
    def __init__(self, term, index, command):
        self.term = term
        self.index = index
        self.command = command

    def __repr__(self):
        output_str_list = []
        output_str_list.append(f"Term: {self.term}")
        output_str_list.append(f"Index: {self.index}")
        output_str_list.append(f"Command: < {self.command} >")

        return "\n" + " ".join(output_str_list)


class State:
    lock = threading.RLock()
    save_dir = 'saved_states'

    def __init__(self, curr_term, voted_for, log: List[LogEntry]):
        self.curr_term = curr_term
        self.voted_for = voted_for
        self.log = log
        self.counter = 0
        self.client_req_respond_count = dict()
        self.commit_idx = 0
        self.group_counter = 0

    def append_to_log(self, command):
        with State.lock:
            self.log.append(LogEntry(self.curr_term, self.get_last_log_index()+1, command))

    def get_last_log_index(self):
        with State.lock:
            return self.log[-1].index

    def get_last_log_term(self):
        with State.lock:
            return self.log[-1].term

    def trim_logs_to_index(self, prev_log_idx):
        with State.lock:
            while self.get_last_log_index() >= prev_log_idx:
                self.log.pop()

    def increment_and_get_client_counter(self):
        self.counter += 1
        return self.counter

    # def req_in_logs(self, request_id):
    #     print("acquiring lock req_in_logs")
    #     with State.lock:
    #         print("lock acquired")
    #         for log_entry in self.log:
    #             if log_entry.command.request_id == request_id:
    #                 return True
    #         print("lock released")
    #         return False

    def update_response_count_since(self, peer_last_index, majority, peer_id):
        update_from_index = max(self.commit_idx, peer_last_index) + 1
        commit_state_updated = False
        with State.lock:
            for log_entry in self.log[update_from_index:]:
                self.client_req_respond_count[log_entry.command[REQUEST_ID]].add(peer_id)
                if len(self.client_req_respond_count[log_entry.command[REQUEST_ID]]) >= majority \
                    and log_entry.term == self.curr_term:
                    self.commit_idx = max(self.commit_idx, log_entry.index)
                    commit_state_updated = True
        return commit_state_updated

    def update_commit_idx(self, commit_idx):
        # TODO :  what else to do in commit index
        with self.lock:
            self.commit_idx = commit_idx



class Event(enum.Enum):
    REQUEST_VOTE = 1
    RESPOND_VOTE = 2
    APPEND_ENTRY = 3
    RESPOND_ENTRY = 4
    EOM = 5
    NEW_GROUP = 6
    NEW_COMMAND = 7
    NEW_CONNECTION = 8
    TEST_CONNECTION = 9
    COMMAND_SUCCESS_NOTIFICATION = 10
    CLIENT_REQUEST = 11


class Group:

    def __init__(self, group_id, public_key, private_key, client_id_list):
        self.group_id = group_id
        self.public_key = public_key
        self.private_key = private_key
        self.client_id_list = client_id_list
        self.messages = []

    def __repr__(self):
        output_str_list = []
        output_str_list.append(f"group_id: {self.group_id}")
        output_str_list.append(f"public_key: {self.public_key}")
        output_str_list.append(f"private_key: < {self.private_key} >")
        output_str_list.append(f"client_id_list: < {self.client_id_list} >")
        output_str_list.append(f"Private key: < {self.messages} >")

        return "\n" + " ".join(output_str_list)

    
class Log_entry_type(enum.Enum):
    CREATE_ENTRY = 1
    ADD_ENTRY = 2
    KICK_ENTRY = 3
    MESSAGE_ENTRY = 4
    DUMMY = 5

# class Command:
#     def __init__(self, request_id, cmd):
#         self.request_id = request_id
#         self.cmd = cmd
#
#     def __repr__(self):
#         output_str_list = []
#         output_str_list.append(f"cmd: {self.cmd}")
#         output_str_list.append(f"request id: {self.request_id}")
#
#         return ", ".join(output_str_list)


