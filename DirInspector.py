import logging
import os
import sys
import threading
import win32file
from multiprocessing import Queue

import win32con

from FileHandler import FileHandler
from TableHandler import TableHandler


class DirInspector:
    logger = logging.getLogger("DirInspector")

    MAIN_QUEUE = Queue()

    ACTIONS = {
        1: "Created",
        2: "Deleted",
        3: "Updated",
        4: "Renamed from",
        5: "Renamed to"
    }

    FILE_LIST_DIRECTORY = 0x0001

    def __init__(self, path):
        self.table_handler = TableHandler()
        self.path_to_watch = path
        self.hDir = win32file.CreateFile(
            self.path_to_watch,
            self.FILE_LIST_DIRECTORY,
            win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE | win32con.FILE_SHARE_DELETE,
            None,
            win32con.OPEN_EXISTING,
            win32con.FILE_FLAG_BACKUP_SEMANTICS,
            None
        )

    def create_clusters(self):
        self.table_handler = TableHandler("teach_table.csv")
        self.table_handler.create_cluster_table(FileHandler.collect_information(self.__first_generation()))

    def run(self):
        self.table_handler.create_table(FileHandler.collect_information(self.__first_generation()))
        q = threading.Thread(target=self.queue_handler)
        q.setDaemon(True)
        t = threading.Thread(target=self.inspect)
        q.start()
        t.start()
        t.join()
        pass

    def __first_generation(self):
        file_list = []
        for root_dir, dirs, files in os.walk(self.path_to_watch):
            for file in files:
                if file[-4:] == ".txt":
                    file_list.append(str(os.path.join(root_dir, file)))
        return file_list

    def inspect(self):
        while True:
            # ReadDirectoryChangesW takes a previously-created
            # handle to a directory, a buffer size for results,
            # a flag to indicate whether to watch subtrees and
            # a filter of what changes to notify.
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
            self.MAIN_QUEUE.put(results)
            print("put")
            pass
        pass

    def queue_handler(self):
        while True:
            results = self.MAIN_QUEUE.get()
            print("get")
            for action, file in results:
                global temp
                full_filename = os.path.join(self.path_to_watch, file)

                if full_filename[-4:] != ".txt":
                    continue

                self.logger.info(str(full_filename) + "| is: " + str(self.ACTIONS.get(action)))
                if action == 2:
                    self.table_handler.delete(full_filename)
                    pass
                elif action == 3:
                    self.table_handler.update(full_filename)
                    pass
                elif action == 4:
                    temp = full_filename
                    pass
                elif action == 5:
                    if sys.getsizeof(full_filename) != 0:
                        self.table_handler.rename(temp, full_filename)
                        temp = None
                    pass
                pass
            pass
        pass
    pass

# if __name__ == "__main__":
#     root_path = check_args()
#     main_class = DirInspector(root_path)
