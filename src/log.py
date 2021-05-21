import threading
from enum import Enum


class Operation(Enum):
    GET = 1
    PUT = 2


class Command:
    def __init__(self, operation, message=None):
        self.__operation = operation
        self.__message = message

    @property
    def operation(self):
        return self.__operation

    @property
    def message(self):
        return self.__operation


class LogEntry:
    def __init__(self, term, command):
        self.__term = term
        self.__command = command

    @property
    def term(self):
        return self.__term

    @property
    def command(self):
        return self.__command


class NodeLog:
    def __init__(self):
        self.__thread_lock = threading.Lock()
        self.__entries = []
        self.__committed_index = -1
        self.__applied_index = -1

    @property
    def committed_index(self):
        return self.__committed_index

    @property
    def applied_index(self):
        return self.__applied_index

    @property
    def log_size(self):
        return len(self.__entries)

    @property
    def entries(self):
        return self.__entries

    def delete_entries_from(self, idx):
        self.__entries = self.__entries[:idx]

    def append(self, entry):
        self.__entries.append(entry)
