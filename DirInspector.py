import logging
import os
import sys
import threading
import win32file
from shutil import copyfile

import win32con

from FileHandler import FileHandler
from TableHandler import TableHandler

# Log-control block
if os.path.exists(str(os.getcwd()) + ".\logs\my_watch.log") and os.path.getsize(".\logs\my_watch.log") > 100240:
    copyfile(".\logs\my_watch.log", ".\logs\old_my_watch.log")
    os.remove(str(os.getcwd()) + ".\logs\my_watch.log")

# Logger initialization.
logger = logging.getLogger("DirInspector")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(".\logs\my_watch.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s -> %(message)s'))
logger.addHandler(file_handler)


class DirInspector:
    ACTIONS = {
        1: "Created",
        2: "Deleted",
        3: "Updated",
        4: "Renamed from",
        5: "Renamed to"
    }

    FILE_LIST_DIRECTORY = 0x0001

    def __init__(self, path):
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
        inspector_thread = threading.Thread(target=self.run())
        inspector_thread.start()
        pass

    def run(self):
        FileHandler.collect_information(self.__first_generation())
        self.__inspect()
        pass

    def __first_generation(self):
        file_list = []
        for root_dir, dirs, files in os.walk(self.path_to_watch):
            for file in files:
                if file[-4:] == ".txt":
                    file_list.append(str(os.path.join(root_dir, file)))
        return file_list

    def __inspect(self):
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

            for action, file in results:
                global temp
                full_filename = os.path.join(self.path_to_watch, file)

                if full_filename[-4:] != ".txt":
                    continue

                logger.info(str(full_filename) + "| is: " + str(self.ACTIONS.get(action)))
                if action == 1:
                    threading.Thread(target=TableHandler.create(full_filename)).start()
                elif action == 2:
                    threading.Thread(target=TableHandler.delete(full_filename)).start()
                    pass
                elif action == 3:
                    threading.Thread(target=TableHandler.update(full_filename)).start()
                    pass
                elif action == 4:
                    temp = full_filename
                    pass
                elif action == 5:
                    threading.Thread(target=TableHandler.rename(temp, full_filename))
                    temp = None
                    pass
                pass
            pass
        pass


def check_args():
    if len(sys.argv) != 2:
        logger.error("Have not enough args, except one: dir_for_watching \n"
                     "Launch script again, and don't forget about args")
        sys.exit(-1)
        pass
    if os.path.exists(sys.argv[1]) is False:
        logger.error("Directory does not exist, Launch script again, and don't forget about args")
        sys.exit(-1)
        pass
    logger.info('My watch begin. Watching for ' + str(sys.argv[1]))
    return sys.argv[1]


if __name__ == "__main__":
    root_path = check_args()
    main_class = DirInspector(root_path)
