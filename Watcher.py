import logging
import os
import sys
import threading
import win32file
from multiprocessing import Queue

import win32con

from FileHandler import FileHandler
from TableHandler import TableHandler


class Watcher:
    number = 0

    ACTIONS = {
        1: "Created",
        2: "Deleted",
        3: "Updated",
        4: "Renamed from",
        5: "Renamed to"
    }

    FILE_LIST_DIRECTORY = 0x0001

    def __init__(self, target_path: str, is_learn: bool):
        self.target_path = target_path
        self.hDir = win32file.CreateFile(
            self.target_path,
            self.FILE_LIST_DIRECTORY,
            win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE | win32con.FILE_SHARE_DELETE,
            None,
            win32con.OPEN_EXISTING,
            win32con.FILE_FLAG_BACKUP_SEMANTICS,
            None
        )
        self.logger = self.__get_logger(target_path)
        self.queue = Queue()
        if is_learn:
            self.table_handler = TableHandler(is_learn, "teach_table.csv")
        else:
            self.table_handler = TableHandler(is_learn, "table" + str(++Watcher.number) + ".csv")
        self.logger.info(
            "Object watcher was created. Watching for " + target_path + ", it's learner watcher = " + str(is_learn))

    def run(self):
        self.table_handler.create_table(FileHandler.collect_information(self.__first_generation()))
        q = threading.Thread(target=self.queue_handler)
        q.setDaemon(True)
        t = threading.Thread(target=self.watch)
        t.start()
        q.start()
        # t.join()
        pass

    # def run_watching(self):
    #     self.table_handler.create_table(FileHandler.collect_information(self.__first_generation()))
    #     q = threading.Thread(target=self.queue_handler)
    #     q.setDaemon(True)
    #     t = threading.Thread(target=self.watch)
    #     t.start()
    #     q.start()
    #     # t.join()
    #     pass
    #
    # def run_learning(self):
    #     self.table_handler.create_table(FileHandler.collect_information(self.__first_generation()))
    #     # self.cluster_handler.make_cluster()
    #     q = threading.Thread(target=self.queue_handler)
    #     q.setDaemon(True)
    #     t = threading.Thread(target=self.watch)
    #     t.start()
    #     q.start()
    #     # t.join()
    #     pass

    def __first_generation(self):
        file_list = []
        for root_dir, dirs, files in os.walk(self.target_path):
            for file in files:
                if file[-4:] == ".txt":
                    file_list.append(str(os.path.join(root_dir, file)))
        return file_list

    def watch(self):
        while True:
            results = win32file.ReadDirectoryChangesW(
                self.hDir,
                4096,
                True,
                win32con.FILE_NOTIFY_CHANGE_FILE_NAME |
                win32con.FILE_NOTIFY_CHANGE_DIR_NAME |
                win32con.FILE_NOTIFY_CHANGE_ATTRIBUTES |
                win32con.FILE_NOTIFY_CHANGE_SIZE |
                win32con.FILE_NOTIFY_CHANGE_LAST_WRITE |
                win32con.FILE_NOTIFY_CHANGE_SECURITY,
                None,
                None
            )

            self.queue.put(results)
            self.logger.info("Some changes was happened")
            pass
        pass

    def queue_handler(self):
        while True:
            results = self.queue.get()
            old_name = ""
            for action, file in results:
                full_filename = os.path.join(self.target_path, file)
                if full_filename[-4:] != ".txt":
                    continue

                self.logger.info(full_filename + "| is: " + str(self.ACTIONS.get(action)))
                if action == 2:
                    self.table_handler.delete(full_filename)

                elif action == 3:
                    self.table_handler.update(full_filename)

                elif action == 4:
                    old_name = full_filename

                elif action == 5:
                    if sys.getsizeof(full_filename) != 0:
                        self.table_handler.rename(old_name, full_filename)
                        old_name = ""

    # noinspection PyMethodMayBeStatic
    def __get_logger(self, logger_name: str):
        logger = logging.getLogger("Watcher " + logger_name)
        logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler(".\logs\my_watch.log")
        fh.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'))
        logger.addHandler(fh)
        return logger
