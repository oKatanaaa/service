import win32file
from multiprocessing import Queue
from threading import Thread

import win32con


class DirWatcher(Thread):
    FILE_LIST_DIRECTORY = 0x0001

    def __init__(self, target_path, queue: Queue):
        super().__init__()
        self.setName("Watcher thread " + str(self.ident))
        self.setDaemon(True)
        self.target_path = target_path
        self.queue = queue

        self.hDir = win32file.CreateFile(
            self.target_path,
            self.FILE_LIST_DIRECTORY,
            win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE | win32con.FILE_SHARE_DELETE,
            None,
            win32con.OPEN_EXISTING,
            win32con.FILE_FLAG_BACKUP_SEMANTICS,
            None
        )

    def run(self):
        self.start_watch()

    def start_watch(self):
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

    pass
