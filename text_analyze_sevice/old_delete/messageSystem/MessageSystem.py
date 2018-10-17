import logging
from multiprocessing import Queue

from messageSystem.Message import Message


class MessageSystem:
    ADDRESS_LIST = {
        "Watcher": 0,
        "FileHandler": 1,
        "TableHandler": 2,
        "ClusterHandler": 3,
        "Utility": 4,
        "Interrupt": 5
    }

    def __init__(self):
        self.logger = self.__get_logger()
        self.queue_listing = [list(), list(), list(), list(), list(), list()]
        self.logger.info("Class " + self.__class__.__name__ + " successfully initialized")

    def register(self, new_handler):
        # Need to catch exception
        self.queue_listing[self.ADDRESS_LIST[new_handler.__class__.__name__]].append(Queue())

    def root_register(self):
        self.queue_listing[self.ADDRESS_LIST["Utility"]].append(Queue())
        self.queue_listing[self.ADDRESS_LIST["Interrupt"]].append(Queue())

    def send(self, message: Message):
        # Need to catch exception
        # Choice easy queue
        self.queue_listing[message.to][0].put(message.msg)
        self.logger.info("Sending message")

    def all_tasks_done(self):
        for queue in self.queue_listing:
            if not queue[0].empty:
                return False
        return True

    def __get_logger(self):
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler(".\logs\my_watch.log")
        fh.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'))
        logger.addHandler(fh)
        return logger