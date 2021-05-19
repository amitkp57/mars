import random
import time
from enum import Enum


class Role(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class Node:
    def __init__(self, sibling_nodes):
        self.__role = Role.FOLLOWER
        self.__leader = None
        self.__last_heartbeat = time.perf_counter()
        self.__timeout = random.uniform(0, 10)
        self.__sibling_nodes = sibling_nodes

    @property
    def role(self):
        return self.__role

    @property
    def total_nodes(self):
        return len(self.__sibling_nodes) + 1

    @property
    def sibling_nodes(self):
        return self.__sibling_nodes

    def transition_to_new_role(self, role):
        self.__role = role
        return

    def check_heartbeat_timeout(self):
        """
        Checks if time elapsed is greater than the timeout.
        If yes, reset timer.
        :return: boolean
        """
        cur_time = time.perf_counter()
        time_elapsed = cur_time - self.__last_heartbeat
        print(f'time elapsed: {time_elapsed}')
        if time_elapsed >= self.__timeout:
            self.reset_last_heartbeat()
            return True
        return False

    def reset_last_heartbeat(self):
        """
        Resets last heartbeat received time to current time.
        """
        self.__last_heartbeat = time.perf_counter()
        self.__timeout = random.uniform(0, 10)


if __name__ == '__main__':
    node = Node()
    while True:
        print(node.check_heartbeat_timeout())
        time.sleep(1)
