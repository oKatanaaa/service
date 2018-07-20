import sys
from threading import Thread
from multiprocessing import Queue
import logging
import DirInspector


class MessageSystem(Thread):
    queue = Queue()
    logger = logging.getLogger("Message system")

    def __init__(self):
        Thread.__init__(self)
        self.daemon = True
        self.start()

    def run(self):

    def put(self):
        pass

    def send(self):
        pass

class Address:
    