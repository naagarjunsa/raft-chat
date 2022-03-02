import enum
import threading
from typing import List


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
        output_str_list.append(f"Command: {self.command}")

        return "\n" + " ".join(output_str_list)

class State:
    lock = threading.Lock()

    def __init__(self, curr_term, voted_for, log: List[LogEntry]):
        self.curr_term = curr_term
        self.voted_for = voted_for
        self.log = log
        self.counter = 0
        self.client_req_respond_count = dict()
        self.commit_idx = 0

    def append_to_log(self, term, command):
        with State.lock:
            self.log.append(LogEntry(term, self.get_last_log_index(), command))

    def get_last_log_index(self):
        with State.lock:
            return self.log[-1].index

    def get_last_log_term(self):
        with State.lock:
            return self.log[-1].term

    def trim_logs_to_index(self, prev_log_idx):
        with State.lock:
            while self.get_last_log_index() != prev_log_idx:
                self.log.pop()

    def increment_and_get_client_counter(self):
        self.counter += 1
        return self.counter

    def req_in_logs(self, request_id):
        with State.lock:
            for log_entry in self.log:
                if log_entry.command.request_id == request_id:
                    return True
            return False

    def update_response_count_since(self, peer_last_index):
        update_from_index = max(self.commit_id)
        with State.lock:
            for log_entry in self.log[peer_last_index]:



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


class Command:
    def __init__(self, request_id, cmd):
        self.request_id = request_id
        self.cmd = cmd

    def __repr__(self):
        output_str_list = []
        output_str_list.append(f"cmd: {self.cmd}")
        output_str_list.append(f"request id: {self.request_id}")

        return "\n" + " ".join(output_str_list)


