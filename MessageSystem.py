import logging
from multiprocessing import Queue
from threading import Thread


class MessageSystem(Thread):
    queue = Queue()
    logger = logging.getLogger("Message system")

    def __init__(self):
        Thread.__init__(self)
        self.daemon = True
        self.start()

    def run(self):
        pass

    def put(self):
        pass

    def send(self):
        pass

class Address:
    pass
