import logging
from threading import Thread

import pandas as pd

from messageSystem.Utils import *
from text_analyze_sevice.handlers.Watcher import Watcher
from messageSystem.Message import Message

"""
Класс обработчик таблицы. Создаёт, добавляет, удаляет, редактирует записи.
Поток демон
"""


# noinspection PyMethodMayBeStatic
class TableHandler(Thread):

    def __init__(self, message_system):
        Thread.__init__(self)
        self.cls_size = 0
        pd.options.mode.chained_assignment = None
        self.setName("Table Handler Daemon Thread")
        self.setDaemon(True)
        self.logger = self.__get_logger()
        """ Ссылка на общий для всех объект - системы сообщений     """
        self.message_system = message_system
        """ Адрес класса в системе сообщений """
        self.address = message_system.ADDRESS_LIST[self.__class__.__name__]
        """ Не используется """
        self.number_of_queue = len(message_system.queue_listing[self.address]) - 1

        self.logger.debug("Class " + self.__class__.__name__ + " successfully initialized")

    """
    Обработка сообщений
    """

    def run(self):
        while True:
            msg = self.message_system.queue_listing[self.address][0].get()
            self.logger.debug(self.__class__.__name__ + " have a message " + msg[option])

            if msg[option] == create_table_option:
                self.create_table(msg)
            elif msg[option] == create_teacher_tables_option:
                self.create_teacher_table(msg)
            elif msg[option] == update_table_option:
                self.update_table(msg)
            elif msg[option] == rename_option:
                self.rename(msg)
            elif msg[option] == delete_from_table_option:
                self.delete(msg)
                self.__lock()
                if self.cls_size == 10:
                    new_msg = Message(
                        self.address,
                        self.message_system.ADDRESS_LIST['Utility'],
                        {}
                    )
                    self.message_system.send(new_msg)
            elif msg[option] == prepare_to_update_option:
                self.prepare_to_update(msg)
                self.__lock()
            elif msg[option] == "kill":
                break
            else:
                self.logger.error("Unknown option in " + self.__class__.__name__)

    """
    Барьер или блокировщик. К сожалению после каждого обновления кластеров должна обновится таблица
    файлов. Для этого можно использовать deque(?) или этот блок
    """

    def __lock(self):
        self.logger.debug("TableHandler locked")
        msg = self.message_system.queue_listing[self.message_system.ADDRESS_LIST["Interrupt"]][0].get()
        self.update_table(msg)
        while not self.message_system.queue_listing[self.message_system.ADDRESS_LIST["Interrupt"]][0].empty:
            msg = self.message_system.queue_listing[self.message_system.ADDRESS_LIST["Interrupt"]][0].get()
            self.update_table(msg)
        self.logger.debug("TableHandler UNlocked!")

    """
    Создание таблицы, в которой отсутствует столбец cluster
    """

    def create_teacher_table(self, msg):
        name_of_table = msg[table_name]
        file_contents = msg[files_with_content]
        file = pd.DataFrame({"path": [], "last_symbol": []})
        file.columns = ['path', 'last_symbol']
        for line in file_contents:
            file.loc[len(file)] = line
        file.to_csv(name_of_table, index=False)
        self.logger.debug("Teach table was created")

    """
    Создание полной таблицы
    """

    def create_table(self, msg):
        name_of_table = msg[table_name]
        file_contents = msg[files_with_content]
        file = pd.DataFrame({"path": [], "last_symbol": [], "cluster": []})
        file.columns = ['path', 'last_symbol', 'cluster']
        for line in file_contents:
            file.loc[len(file)] = line
        file.to_csv(name_of_table, index=False)
        self.logger.debug("Table was created")

    """
    Обработка переименования
    """

    def rename(self, msg):
        name_of_table = msg[table_name]
        old_file_name = msg[old_name]
        new_file_name = msg[new_name]
        file = pd.read_csv(name_of_table, sep=',')
        file.path[file.path == old_file_name] = new_file_name
        file.to_csv(name_of_table, index=False)
        self.logger.debug("Renamed some rows")

    """
    Обработка удаления.
    Если был удалён файл не из обучающей выборки - то мы просто удаляем запись.
    Если удалён файл обучающей выборки, то нужно проверить, только ли удалённый файл заканчивался на какой то символ,
    Если не только он, то спокойно удаляем запись из таблицы, если такой символ был единственный, а значит и такой 
    кластер задаётся только этим файлом, то нам нужно удалить кластер. Но перед этим, пока мы работаем с таблицей,
    нужно проверить все открытые файлы (не учителя) и для каждой строки, которая помечена удалённым кластером,
    нужно произвести обновление кластера. Поэтому дальше работа передается обработчику кластеров. А затем,
    обновление таблиц будет произведено в методе этого класса.
    """

    def delete(self, msg):
        self.logger.debug("TableHandler delete start work!")
        name_of_table = msg[table_name]
        teacher = msg[is_teacher]
        target_path = msg[path]
        if not teacher:
            file = pd.read_csv(name_of_table, sep=',')
            file = file[file.path != target_path]
            file.to_csv(name_of_table, index=False)
            # При удалении файлов нужно послать сигнал, что бы снять лок. В этом случае он бесполезный
            new_msg = Message(
                self.address,
                self.message_system.ADDRESS_LIST['Interrupt'],
                {option: update_table_option,
                 files_with_content: [],
                 table_name: None,
                 is_teacher: None}
            )
            self.message_system.send(new_msg)
        else:
            file = pd.read_csv(name_of_table, sep=',')
            try:
                cluster_of_deleted_file = file.last_symbol[file.path == target_path].iloc[0][0]
            except IndexError:
                # Фантомные боли. Попытка удалить уже удаленный кластер
                # Или попытка удалить несуществующий вообще кластер
                # @see prepare_to_update
                self.logger.warning("The table doesn't have this file")
                return
            # Проверяем что файл с таким кластером единственный
            need_to_make_changes_after_remove = len(file.last_symbol[file.last_symbol == cluster_of_deleted_file]) == 1
            file = file[file.path != target_path]
            file.to_csv(name_of_table, index=False)
            if need_to_make_changes_after_remove:
                # Удаляем из списка класеров
                new_msg = Message(
                    self.address,
                    self.message_system.ADDRESS_LIST["ClusterHandler"],
                    {option: delete_from_clusters_option,
                     deleted_cluster: cluster_of_deleted_file}
                )
                # Переменная нужная лишь для тестирования
                # TODO: Убрать это
                self.cls_size += 1
                self.message_system.send(new_msg)

                for table in Watcher.OPENED_TABLES:
                    name_of_table = table[table_name]
                    teacher = table[is_teacher]
                    if not teacher:
                        table = pd.read_csv(name_of_table, sep=',')
                        try:
                            need_to_update = table[table.cluster == cluster_of_deleted_file]
                        except TypeError:
                            self.logger.debug("Clusters of files not yet defined")
                            return
                        need_to_update = need_to_update.to_dict()
                        keys = need_to_update['path'].keys()
                        lines = []
                        for key in keys:
                            lines.append({'path': need_to_update['path'][key],
                                          'last_symbol': need_to_update['last_symbol'][key],
                                          'cluster': need_to_update['cluster'][key]})
                        if len(lines) > 0:
                            new_msg = Message(
                                self.address,
                                self.message_system.ADDRESS_LIST["ClusterHandler"],
                                {option: update_after_delete,
                                 deleted_cluster: cluster_of_deleted_file,
                                 table_name: name_of_table,
                                 is_teacher: False,
                                 files_with_content: lines}
                            )
                            self.message_system.send(new_msg)
                        else:
                            # Используется для unlock'a
                            new_msg = Message(
                                self.address,
                                self.message_system.ADDRESS_LIST['Interrupt'],
                                {option: update_table_option,
                                 files_with_content: [],
                                 table_name: None,
                                 is_teacher: None}
                            )
                            self.message_system.send(new_msg)
            # Если не было изменений в таблице, нужно все равно снять лок
            else:
                new_msg = Message(
                    self.address,
                    self.message_system.ADDRESS_LIST['Interrupt'],
                    {option: update_table_option,
                     files_with_content: [],
                     table_name: None,
                     is_teacher: None}
                )
                self.message_system.send(new_msg)
        self.logger.debug("TableHandler delete end his work")

    """
    Обновление таблицы
    Здесь мы обновляем записи в таблице.
    Возможно стоит заменить pandas на что то другое, при большом количестве файлов
    работает оооооочень медлено
    """

    def update_table(self, msg):
        self.logger.debug("Update_table start")
        name_of_table = msg.get(table_name)
        # TODO: check this
        if name_of_table is None: return
        teacher = msg[is_teacher]

        files = msg[files_with_content]

        if len(files) == 0:
            return

        file = pd.read_csv(name_of_table, sep=',')
        for row in files:
            if file[file.path == row["path"]].empty:
                file.loc[len(file)] = row
            else:
                file.path[file["path"] == row["path"]] = row['path']
                file.last_symbol[file['path'] == row['path']] = row['last_symbol']
                if not teacher:
                    file.cluster[file['path'] == row['path']] = row['cluster']

        file.to_csv(name_of_table, index=False)
        self.logger.debug("Now table have some update")

    """
    Подготовка к обновлению таблицы.
    Метод вызывается если был обновлен файл с кластером (добавлен или изменён)
    """

    def prepare_to_update(self, msg):
        self.logger.debug("Prepare_to_update starts")
        name_of_table = msg[table_name]
        files = msg[files_with_content]

        if len(files) == 0:
            return

        for file in files:
            new_msg = {
                path: file['path'],
                table_name: name_of_table,
                is_teacher: True
            }
            self.delete(new_msg)

        # Обновление таблицы кластеров
        new_msg = {
            table_name: name_of_table,
            is_teacher: True,
            files_with_content: files
        }
        self.update_table(new_msg)

        # Добавление новго кластера. backtrack - запрос соседей нового кластера
        new_msg = Message(
            self.address,
            self.message_system.ADDRESS_LIST["ClusterHandler"],
            {option: create_clusters_option,
             files_with_content: files,
             'backtrack': True}
        )
        self.message_system.send(new_msg)

        new_cls = files[0]['last_symbol']
        for file in Watcher.OPENED_TABLES:
            file_name = file["table_name"]
            teacher = file["is_teacher"]

            if not teacher:
                file = pd.read_csv(file_name, sep=',')

                file = file.to_dict(orient='records')

                # Проверяем только те файлы что являются соседями новго кластера
                new_msg = Message(
                    self.address,
                    self.message_system.ADDRESS_LIST["ClusterHandler"],
                    {option: soft_update_option,
                     table_name: file_name,
                     is_teacher: False,
                     'new_cluster': new_cls,
                     files_with_content: file}
                )

                self.message_system.send(new_msg)
        self.logger.debug("Prepare_to_update end")

    def __get_logger(self):
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler(".\logs\my_watch.log")
        fh.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'))
        logger.addHandler(fh)
        return logger


def distance(first, second):
    return abs(ord(first) - ord(second))
