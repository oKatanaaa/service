from multiprocessing import Queue

from messageSystem.Message import Message


class MessageSystem:
    ADDRESS_LIST = {
        "Watcher": 0,
        "FileHandler": 1,
        "TableHandler": 2,
        "ClusterHandler": 3
    }

    def __init__(self):
        self.queue_listing = [list(), list(), list(), list()]

    def register(self, new_handler):
        # Need to catch exception
        self.queue_listing[self.ADDRESS_LIST[new_handler.__class__.__name__]].append(Queue())

    def send(self, message: Message):
        # Need to catch exception
        # Choice easy queue
        self.queue_listing[message.to][0].put(message.msg)
