import logging
import os
import sys
from threading import Thread

from handlers.Utils import *
from messageSystem.Message import Message

"""
Класс обработчик файловых событий.
Предназначен для работы с файлами целевой директории.
Наследует Thread - чтоб работал в отдельном потоке. (поток демон)
"""


# noinspection PyMethodMayBeStatic
class FileHandler(Thread):
    """ Вспомогательная карта """
    ACTIONS = {
        1: "Created",
        2: "Deleted",
        3: "Updated",
        4: "Renamed from ",
        5: "Renamed to "
    }

    def __init__(self, message_system):
        Thread.__init__(self)
        self.setName("File Handler Daemon Thread")
        self.setDaemon(True)
        """ Дополнительное поле, используется при переименовании """
        self.temp = None
        self.logger = self.__get_logger()
        """ Ссылка на общий для всех объект - системы сообщений     """
        self.message_system = message_system
        """ Адрес класса в системе сообщений """
        self.address = message_system.ADDRESS_LIST[self.__class__.__name__]
        """ Не используемое поле """
        self.number_of_queue = len(message_system.queue_listing[self.address]) - 1
        self.logger.info("Class " + self.__class__.__name__ + " successfully initialized")

    """
    Метод запускаемый при старте потока. Бесконечно ожидает переданных ему сообщений
    Если получает, то обрабатывает их и вызывает нужные функции
    """
    def run(self):
        while True:
            msg = self.message_system.queue_listing[self.address][0].get()
            self.logger.info(self.__class__.__name__ + " have a message")

            if msg[option] == first_generation_option:
                self.first_generation(msg)
            elif msg[option] == changes_option:
                self.get_changes(msg)

    """
    Первая генерация. Создаёт список файлов и их содержимого
    Вызывает нужные методы в зависимости от того, дирректория обучения это или нет
    """
    def first_generation(self, msg):
        file_name_list = []
        target_path = msg[path]
        for root_dir, dirs, files in os.walk(target_path):
            for file in files:
                if file[-4:] == ".txt":
                    file_name_list.append(os.path.join(root_dir, file))

        file_contents = self.grub_file_content(file_name_list)
        if msg[is_teacher]:
            new_msg = Message(
                self.address,
                self.message_system.ADDRESS_LIST["ClusterHandler"],
                {option: create_clusters_option,
                 table_name: msg[table_name],
                 files_with_content: file_contents}
            )
            additional_msg = Message(
                self.address,
                self.message_system.ADDRESS_LIST["TableHandler"],
                {option: create_teacher_tables_option,
                 table_name: msg[table_name],
                 files_with_content: file_contents}
            )
            self.message_system.send(additional_msg)
        else:
            new_msg = Message(
                self.address,
                self.message_system.ADDRESS_LIST["ClusterHandler"],
                {option: identify_clusters_option,
                 table_name: msg[table_name],
                 files_with_content: file_contents}
            )
        self.message_system.send(new_msg)
        self.logger.info("Table successfully generated")

    """
    Метод вызываемый при изменении какого-либо файла целевой директории
    В зависимости от изменений вызывает соответсвующие им методы
    """
    def get_changes(self, msg):
        target_path = msg[path]
        changes_list = msg[changes]
        teacher = msg[is_teacher]
        for action, file in changes_list:
            if file[-4:] != ".txt":
                continue

            full_filename = os.path.join(target_path, file)
            self.logger.info(file + ": is " + self.ACTIONS[action])

            if action == 2:
                new_msg = Message(
                    self.address,
                    self.message_system.ADDRESS_LIST["TableHandler"],
                    {option: delete_from_table_option,
                     is_teacher: teacher,
                     table_name: msg[table_name],
                     path: full_filename}
                )
                self.message_system.send(new_msg)

            elif action == 3:
                if os.path.getsize(full_filename) != 0:
                    if not teacher:
                        new_msg = Message(
                            self.address,
                            self.message_system.ADDRESS_LIST["ClusterHandler"],
                            {option: update_cluster_option,
                             is_teacher: teacher,
                             is_deleted: False,
                             table_name: msg[table_name],
                             files_with_content: self.grub_file_content([full_filename])}
                        )
                    else:
                        new_msg = Message(
                            self.address,
                            self.message_system.ADDRESS_LIST["TableHandler"],
                            {option: prepare_to_update_option,
                             is_teacher: teacher,
                             table_name: msg[table_name],
                             files_with_content: self.grub_file_content([full_filename])}
                        )
                    self.message_system.send(new_msg)

            elif action == 4:
                self.temp = full_filename

            elif action == 5:
                if sys.getsizeof(full_filename) != 0:
                    new_msg = Message(
                        self.address,
                        self.message_system.ADDRESS_LIST["TableHandler"],
                        {option: rename_option,
                         table_name: msg[table_name],
                         old_name: self.temp,
                         new_name: full_filename}
                    )
                    self.message_system.send(new_msg)

    """
    Читает содержимое файла, а именно находит последнюю букву
    """
    def grub_file_content(self, file_name_list):
        file_content = []
        for name in file_name_list:

            try:
                with open(name, 'rb') as file:
                    file.seek(1, os.SEEK_END)
                    while file.tell() > 1 and file.seek(-3, os.SEEK_CUR):
                        ch = file.read(2).decode()[-1]

                        if ch.isalpha() or ch.isdigit() or not ch.isalpha():
                            file_content.append({
                                "path": name,
                                "last_symbol": ch
                            })
                            self.logger.info("File successfully read")
                            break

            except UnicodeDecodeError:
                self.logger.error("Catch UnicodeDecodeError in " + name)
            except PermissionError:
                self.logger.error("Now somewhere used this " + name)
            except OSError:
                self.logger.error("File small, but no so!")
                with open(name, 'rb') as file:
                    ch = file.read(1).decode()
                    if ch.isalpha():
                        file_content.append({
                            "path": name,
                            "last_symbol": ch
                        })
        return file_content

    def __get_logger(self):
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler(".\logs\my_watch.log")
        fh.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'))
        logger.addHandler(fh)
        return logger
