import logging
import win32file
from threading import Thread

import win32con

from messageSystem.Message import Message
from messageSystem.Utils import *


class Watcher(Thread):
    OPENED_TABLES = list()
    OPENED_FILES = list()

    FILE_LIST_DIRECTORY = 0x0001

    def __init__(self, message_system, target_path, _table_name, _is_teacher=False):
        Thread.__init__(self)
        self.setName("Watcher thread")
        self.setDaemon(True)
        self.message_system = message_system
        self.address = message_system.ADDRESS_LIST[self.__class__.__name__]
        self.is_teacher = _is_teacher
        self.target_path = target_path
        self.table_name = _table_name

        Watcher.OPENED_TABLES.append({
            table_name: _table_name,
            is_teacher: _is_teacher
        })
        Watcher.OPENED_FILES.append({
            path: target_path,
            is_teacher: _is_teacher
        })

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
        msg = Message(
            self.address,
            self.message_system.ADDRESS_LIST["FileHandler"],
            {option: first_generation_option,
             is_teacher: self.is_teacher,
             path: self.target_path,
             table_name: self.table_name}
        )
        self.message_system.send(msg)

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

            if not self.message_system.queue_listing[self.address][0]:
                break

            msg = Message(
                self.address,
                self.message_system.ADDRESS_LIST["FileHandler"],
                {option: changes_option,
                 table_name: self.table_name,
                 is_teacher: self.is_teacher,
                 path: self.target_path,
                 changes: results}
            )
            self.message_system.send(msg)
