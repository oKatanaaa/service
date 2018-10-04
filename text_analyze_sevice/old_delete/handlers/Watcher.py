import logging
import win32file
from threading import Thread

import win32con

from handlers.Utils import *
from messageSystem.Message import Message

"""
Класс - дозорные. Ловит любые измнения в целевой папке и успешно передаёт их нужно обработчику
Не является потоком демоном
"""


class Watcher(Thread):
    """
    Список используемых файлов
    """
    OPENED_FILES = list()
    OPENED_TABLES = list()

    FILE_LIST_DIRECTORY = 0x0001

    # TODO: check for existing target_path
    def __init__(self, message_system, target_path, _table_name, _is_teacher=False):
        Thread.__init__(self)
        self.setName("Main Thread (Watcher)")
        """ Ссылка на общий для всех объект - системы сообщений     """
        self.message_system = message_system
        """ Адрес класса в системе сообщений """
        self.address = message_system.ADDRESS_LIST[self.__class__.__name__]
        self.is_teacher = _is_teacher
        self.logger = self.__get_logger()
        self.target_path = target_path
        self.table_name = _table_name
        Watcher.OPENED_TABLES.append({"table_name": _table_name,
                                      "is_teacher": _is_teacher})
        Watcher.OPENED_FILES.append({"path": target_path,
                                     "is_teacher": _is_teacher})
        self.hDir = win32file.CreateFile(
            self.target_path,
            self.FILE_LIST_DIRECTORY,
            win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE | win32con.FILE_SHARE_DELETE,
            None,
            win32con.OPEN_EXISTING,
            win32con.FILE_FLAG_BACKUP_SEMANTICS,
            None
        )
        self.logger.info("Class " + self.__class__.__name__ + " successfully initialized")

    """
    Старт потока. Генерирует первый раз таблицу/кластеры
    """

    def run(self):
        if self.is_teacher:
            msg = Message(
                self.address,
                self.message_system.ADDRESS_LIST["FileHandler"],
                {option: first_generation_option,
                 is_teacher: True,
                 path: self.target_path,
                 table_name: self.table_name}
            )
            self.message_system.send(msg)
        else:
            msg = Message(
                self.address,
                self.message_system.ADDRESS_LIST["FileHandler"],
                {option: first_generation_option,
                 is_teacher: False,
                 path: self.target_path,
                 table_name: self.table_name}
            )
            self.message_system.send(msg)

        self.start_watch()

    """
    Мониторинг
    """

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

            msg = Message(
                self.address,
                self.message_system.ADDRESS_LIST["FileHandler"],
                {option: changes_option,
                 table_name: self.table_name,
                 is_teacher: self.is_teacher,
                 path: self.target_path,
                 changes: results}
            )
            self.logger.info("Some changes was happened")
            self.message_system.send(msg)

    def __get_logger(self):
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler(".\logs\my_watch.log")
        fh.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'))
        logger.addHandler(fh)
        return logger
