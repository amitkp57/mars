import random
import threading
import time
from enum import Enum

from src.log import NodeLog


class Role(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


def get_timeout():
    return random.uniform(100, 500)  # milliseconds


def get_time_millis():
    return time.time() * 1000


class Node:
    def __init__(self, index, sibling_nodes):
        self.__thread_lock = threading.Lock()
        self.__index = index  # server index
        self.__term = -1  # term of the election
        self.__role = Role.FOLLOWER
        self.__voted_for = None  # voted for in current term
        self.__leader = None
        self.__committed_index = -1
        self.__last_applied = -1
        self.__last_heartbeat = get_time_millis()
        self.__timeout = get_timeout()
        self.__sibling_nodes = sibling_nodes
        self.__logs = NodeLog()
        # these two are used when node becomes leader
        self.__next_index = None
        self.__match_index = None

    @property
    def leader(self):
        return self.__leader

    @leader.setter
    def leader(self, leader):
        self.__leader = leader
        return

    @property
    def logs(self):
        return self.__logs

    @property
    def next_index(self):
        return self.__next_index

    @property
    def match_index(self):
        return self.__match_index

    @property
    def committed_index(self):
        return self.__committed_index

    @property
    def last_applied(self):
        return self.__last_applied

    @property
    def voted_for(self):
        return self.__voted_for

    @property
    def index(self):
        return self.__index

    @property
    def term(self):
        return self.__term

    @term.setter
    def term(self, term):
        self.__term = term
        return

    @voted_for.setter
    def voted_for(self, voted_for):
        self.__voted_for = voted_for
        return

    @property
    def role(self):
        return self.__role

    @property
    def total_nodes(self):
        return len(self.__sibling_nodes) + 1

    @property
    def sibling_nodes(self):
        return self.__sibling_nodes

    @property
    def thread_lock(self):
        return self.__thread_lock

    @committed_index.setter
    def committed_index(self, committed_index):
        self.__committed_index = committed_index
        return

    def increment_term(self):
        self.__term += 1
        return

    def transition_to_new_role(self, role):
        self.__role = role
        return

    def is_leader(self):
        return self.__role == Role.LEADER

    def prepare_for_leadership(self):
        self.__match_index = {server: -1 for server in self.__sibling_nodes}
        self.__next_index = {server: self.__logs.log_size() for server in self.__sibling_nodes}

    def check_heartbeat_timeout(self):
        """
        Checks if time elapsed is greater than the timeout.
        If yes, reset timer.
        :return: boolean
        """
        if self.is_leader():  # If leader itself, no need to check heartbeat timeout
            return False
        cur_time = get_time_millis()
        time_elapsed = cur_time - self.__last_heartbeat
        if time_elapsed >= self.__timeout:
            print(f'Heartbeat timeout! Time elapsed since last heartbeat received: {time_elapsed}.')
            self.reset_last_heartbeat()
            return True
        return False

    def reset_last_heartbeat(self):
        """
        Resets last heartbeat received time to current time.
        """
        self.__last_heartbeat = get_time_millis()
        self.__timeout = get_timeout()

    @last_applied.setter
    def last_applied(self, val):
        self.__last_applied = val
        return


if __name__ == '__main__':
    node = Node()
    while True:
        print(node.check_heartbeat_timeout())
        time.sleep(1)
