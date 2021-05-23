import threading
from enum import Enum


class Operation(Enum):
    GET_MESSAGE = 1
    PUT_MESSAGE = 2
    GET_TOPICS = 3
    PUT_TOPIC = 4


class Command():
    def __init__(self, id, operation, message=None):
        self.__id = id
        self.__operation = operation
        self.__message = message

    def __str__(self):
        return f'Command(id="{self.__id}", operation="{self.__operation.name}", message="{self.__message}")'

    @property
    def id(self):
        return self.__id

    @property
    def operation(self):
        return self.__operation

    @property
    def message(self):
        return self.__message

    # json serialization
    def json_encode(self):
        return {
            'id': self.__id,
            'operation': self.__operation.value,
            'message': self.__message
        }

    @classmethod
    def json_decode(cls, json_dict):
        if not json_dict:
            return None
        return Command(json_dict['id'], Operation(json_dict['operation']), json_dict['message'])


class LogEntry():
    def __init__(self, term, command):
        self.__term = term
        self.__command = command

    def __str__(self):
        return f'LogEntry(term="{self.__term}", command="{self.__command}")'

    @property
    def term(self):
        return self.__term

    @property
    def command(self):
        return self.__command

    # json serialization
    def json_encode(self):
        return {
            'term': self.__term,
            'command': None if not self.__command else self.__command.json_encode()
        }

    @classmethod
    def json_decode(cls, json_dict):
        if not json_dict:
            return None
        return LogEntry(json_dict['term'], Command.json_decode(json_dict['command']))


class NodeLog:
    def __init__(self):
        self.__thread_lock = threading.Lock()
        self.__entries = []
        self.__committed_index = -1
        self.__applied_index = -1

    def __str__(self):
        return f'NodeLog(committed_index="{self.__committed_index}", applied_index="{self.__applied_index}", ' \
               f'entries={self.__entries})'

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


if __name__ == '__main__':
    # print(LogEntry.json_decode(LogEntry(3, Command(1, '', '')).json_encode()))
    print(Command(123, Operation.PUT_MESSAGE, 'msg'))
    print(Operation.PUT_TOPIC.name)
    print(Operation.PUT_TOPIC == Operation.PUT_TOPIC)
