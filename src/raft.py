import random
import time
from enum import Enum


class Role(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class Node:
    def __init__(self):
        self.__role = Role.FOLLOWER
        self.__leader = None
        self.__start_time = time.perf_counter()
        self.__timeout = random.uniform(0, 10)
        self.__last_heartbeat = time.perf_counter()

    @property
    def role(self):
        return self.__role

    def reset_last_heartbeat(self):
        """
        Resets last heartbeat received time to current time.
        """
        self.__last_heartbeat = time.perf_counter()

    def reset_timer(self):
        """
        Reset start time to current time and reset timeout value
        """
        self.__start_time = time.perf_counter()
        self.__timeout = random.uniform(0, 10)

    def check_timeout(self):
        """
        Randomly wake up and see if time elapsed is greater than the timeout.
        If yes, reset timer.
        :return: boolean
        """
        cur_time = time.perf_counter()
        time_elapsed = cur_time - self.__start_time
        print(cur_time, self.__start_time, self.__timeout)
        print(f'time elapsed: {time_elapsed}')
        if time_elapsed >= self.__timeout:
            self.reset_timer()
            return True
        return False


if __name__ == '__main__':
    node = Node()
    while True:
        print(node.check_timeout())
        time.sleep(1)
