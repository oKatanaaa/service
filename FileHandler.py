import logging
import os
from threading import Thread

from TableHandler import TableHandler

logger = logging.getLogger("FileHandler")


class FileHandler(Thread):

    def __init__(self, file_list):
        Thread.__init__(self)
        self.file_list = file_list
        self.start()
        self.join()
        pass

    def run(self):
        TableHandler(self.collect_information(self.file_list))
        self.file_list = None
        pass

    # Find last symbol of txt file
    @staticmethod
    def collect_information(file_list):
        logger.info("Collecting content of files started")
        table = []
        for name in file_list:
            if name[-4:] == ".txt":
                if os.path.getsize(name) == 0:
                    table.append({"path": str(name), "last_symbol": "SystemMessage: empty file"})
                else:
                    with open(name, 'rb') as file:
                        file.seek(-1, os.SEEK_END)
                        try:
                            if str(file.readline().decode()).isspace():
                                table.append({"path": name, "last_symbol": "SystemMessage: space_symbol"})
                            else:
                                table.append({"path": name, "last_symbol": file.readline().decode()})
                            logger.info("File read success")
                        except UnicodeDecodeError:
                            table.append({"path": name, "last_symbol": "SystemMessage: can't decode bytes"})
                            logger.error("Catch UnicodeDecodeError in ", name)
                            pass
                        pass
                    pass
                pass
            pass
        return table
