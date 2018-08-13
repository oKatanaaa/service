import logging
from threading import Thread

import pandas as pd

from handlers.Utils import *
from handlers.Watcher import Watcher
from messageSystem.Message import Message

"""
Класс обработчик таблицы. Создаёт, добавляет, удаляет, редактирует записи.
Поток демон
"""


# noinspection PyMethodMayBeStatic
class TableHandler(Thread):

    def __init__(self, message_system):
        Thread.__init__(self)
        self.setName("Table Handler Daemon Thread")
        self.setDaemon(True)
        self.logger = self.__get_logger()
        """ Ссылка на общий для всех объект - системы сообщений     """
        self.message_system = message_system
        """ Адрес класса в системе сообщений """
        self.address = message_system.ADDRESS_LIST[self.__class__.__name__]
        """ Не используется """
        self.number_of_queue = len(message_system.queue_listing[self.address]) - 1
        self.logger.info("Class " + self.__class__.__name__ + " successfully initialized")

    """
    Обработка сообщений
    """

    def run(self):
        while True:
            msg = self.message_system.queue_listing[self.address][0].get()
            self.logger.info(self.__class__.__name__ + " have a message")

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
            elif msg[option] == prepare_to_update_option:
                self.prepare_to_update(msg)

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
        self.logger.info("Teach table was created")

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
        self.logger.info("Table was created")

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
        self.logger.info("Renamed some rows")

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
        name_of_table = msg[table_name]
        teacher = msg[is_teacher]
        target_path = msg[path]
        if not teacher:
            file = pd.read_csv(name_of_table, sep=',')
            file = file[file.path != target_path]
            file.to_csv(name_of_table, index=False)
        else:
            file = pd.read_csv(name_of_table, sep=',')
            cluster_of_deleted_file = file.last_symbol[file.path == target_path].iloc[0][0]
            need_to_make_changes_after_remove = len(file.last_symbol[file.last_symbol == cluster_of_deleted_file]) == 1
            file = file[file.path != target_path]
            file.to_csv(name_of_table, index=False)
            if need_to_make_changes_after_remove:
                for table in Watcher.OPENED_TABLES:
                    name_of_table = table[table_name]
                    teacher = table[is_teacher]
                    if not teacher:
                        table = pd.read_csv(name_of_table, sep=',')
                        need_to_update = table[table.cluster == cluster_of_deleted_file]
                        need_to_update = need_to_update.to_dict()
                        keys = need_to_update['path'].keys()
                        lines = []
                        for key in keys:
                            lines.append({'path': need_to_update['path'][key],
                                          'last_symbol': need_to_update['last_symbol'][key],
                                          'cluster': need_to_update['cluster'][key]})

                        new_msg = Message(
                            self.address,
                            self.message_system.ADDRESS_LIST["ClusterHandler"],
                            {option: deleted_cluster,
                             clusters_list: cluster_of_deleted_file,
                             table_name: name_of_table,
                             files_with_content: lines,
                             is_teacher: teacher,
                             is_deleted: True
                             }
                        )
                        self.message_system.send(new_msg)
                        self.logger.info("I destroyed this")

    """
    Обновление таблицы
    Здесь мы обновляем записи в таблице, в самом простом случае, однако, не всё так просто,
    если произошло обновление обучающего файла, приходится проверять для все файлов (не учителей)
    для каждой строки расстояние от текущего кластера, до нового (или изменённого)
    и соотвественно если расстояние меньше - то производить замену
    """

    def update_table(self, msg):
        name_of_table = msg[table_name]
        teacher = msg[is_teacher]
        if msg[is_deleted]:
            deleted_cls = msg[deleted_cluster]
        else:
            deleted_cls = None

        files = msg[files_with_content]
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

        if teacher:
            new_cluster = files[0]["last_symbol"]
            for file in Watcher.OPENED_TABLES:
                file_name = file["table_name"]
                teacher = file["is_teacher"]

                if not teacher:
                    file = pd.read_csv(file_name, sep=',')
                    soft_update_list = list()
                    hard_update_list = list()

                    for line in range(len(file)):
                        target_path = file.iloc[line, 0]
                        symbol = str(file.iloc[line, 1]).strip()
                        prev_cluster = str(file.iloc[line, 2]).strip()

                        if msg[is_deleted] and prev_cluster in deleted_cls:
                            # new_msg = Message(
                            #     self.address,
                            #     self.message_system.ADDRESS_LIST["ClusterHandler"],
                            #     {option: update_cluster_option,
                            #      is_teacher: False,
                            #      table_name: file_name,
                            #      is_deleted: False,
                            #      files_with_content: [{"path": target_path, "last_symbol": symbol}]}
                            # )
                            # self.message_system.send(new_msg)
                            hard_update_list.append({"path": target_path, "last_symbol": symbol})
                        else:
                            soft_update_list.append({"path": target_path,
                                                     "last_symbol": symbol,
                                                     "cluster": prev_cluster})
                            # if distance(symbol, prev_cluster) > distance(symbol, new_cluster):
                            #     file.cluster[file.path == target_path] = new_cluster
                    if len(hard_update_list) != 0:
                        new_msg = Message(
                            self.address,
                            self.message_system.ADDRESS_LIST["ClusterHandler"],
                            {option: update_cluster_option,
                             is_teacher: False,
                             table_name: file_name,
                             is_deleted: False,
                             files_with_content: hard_update_list}
                        )
                        self.message_system.send(new_msg)
                    if len(soft_update_list) != 0:
                        new_msg = Message(
                            self.address,
                            self.message_system.ADDRESS_LIST["ClusterHandler"],
                            {option: soft_update_option,
                             'new_cluster': new_cluster,
                             is_teacher: False,
                             table_name: file_name,
                             is_deleted: False,
                             files_with_content: soft_update_list}
                        )
                        self.message_system.send(new_msg)
                    file.to_csv(file_name, index=False)
                    self.logger.info("Table have some update")

    def prepare_to_update(self, msg):
        name_of_table = msg["table_name"]
        files = msg[files_with_content]
        table = pd.read_csv(name_of_table, sep=',')
        old_symbol_list = []

        for file in files:
            old_symbol = table.loc[table.path == file["path"]].to_dict(orient='list')
            if len(old_symbol["last_symbol"]) != 0:
                old_symbol_list.append(old_symbol["last_symbol"][0])

        if len(old_symbol_list) > 0:
            new_msg = Message(
                self.address,
                self.message_system.ADDRESS_LIST["ClusterHandler"],
                {option: delete_from_clusters_option,
                 table_name: name_of_table,
                 is_teacher: msg[is_teacher],
                 files_with_content: files,
                 deleted_cluster: old_symbol_list}
            )
        else:
            new_msg = Message(
                self.address,
                self.message_system.ADDRESS_LIST["ClusterHandler"],
                {option: update_cluster_option,
                 table_name: name_of_table,
                 is_teacher: msg[is_teacher],
                 is_deleted: False,
                 files_with_content: files}
            )
        self.message_system.send(new_msg)

    def __get_logger(self):
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler(".\logs\my_watch.log")
        fh.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'))
        logger.addHandler(fh)
        return logger


def distance(first, second):
    return abs(ord(first) - ord(second))
