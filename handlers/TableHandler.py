from threading import Thread

import pandas as pd

from handlers.Watcher import Watcher
from messageSystem.Message import Message


# noinspection PyMethodMayBeStatic
class TableHandler(Thread):

    def __init__(self, message_system):
        Thread.__init__(self)
        self.setName("Table Handler Daemon Thread")
        self.setDaemon(True)
        self.logger = None
        self.message_system = message_system
        self.address = message_system.ADDRESS_LIST[self.__class__.__name__]
        self.number_of_queue = len(message_system.queue_listing[self.address]) - 1

    def run(self):
        while True:
            msg = self.message_system.queue_listing[self.address][self.number_of_queue].get()

            if msg["option"] == "create_table":
                self.create_table(msg)
            elif msg["option"] == "create_teacher_table":
                self.create_teacher_table(msg)
            elif msg["option"] == "update_table":
                self.update_table(msg)
            elif msg["option"] == "rename":
                self.rename(msg)
            elif msg["option"] == "delete":
                self.delete(msg)

    def create_teacher_table(self, msg):
        table_name = msg["table_name"]
        file_contents = msg["file_contents"]
        file = pd.DataFrame({"path": [], "last_symbol": []})
        for line in file_contents:
            file.loc[len(file)] = line
        file.to_csv(table_name, index=False)

    def create_table(self, msg):
        table_name = msg["table_name"]
        file_contents = msg["file_contents"]
        file = pd.DataFrame({"path": [], "last_symbol": [], "cluster": []})
        for line in file_contents:
            file.loc[len(file)] = line
        file.to_csv(table_name, index=False)

    def rename(self, msg):
        table_name = msg["table_name"]
        old_name = msg["old_name"]
        new_name = msg["new_name"]
        file = pd.read_csv(table_name, sep=',')
        file.path[file.path == old_name] = new_name
        file.to_csv(table_name, index=False)

    def delete(self, msg):
        table_name = msg["table_name"]
        is_teacher = msg["is_teacher"]
        path = msg["path"]
        if not is_teacher:
            file = pd.read_csv(table_name, sep=',')
            file = file[file.path != path]
            file.to_csv(table_name, index=False)
        else:
            file = pd.read_csv(table_name, sep=',')
            cluster_of_deleted_file = file.cluster[file.path == path]
            need_to_make_changes_after_remove = len(file.cluster[file.cluster == cluster_of_deleted_file]) == 1
            file = file[file.path != path]
            file.to_csv(table_name, index=False)
            if need_to_make_changes_after_remove:
                for file_name, teacher in Watcher.OPENED_FILES:
                    if not teacher:
                        file = pd.read_csv(file_name, sep=',')
                        need_to_update = file[file.cluster == cluster_of_deleted_file]
                        file.close()
                        new_msg = Message(
                            self.address,
                            self.message_system.ADDRESS_LIST["ClusterHandler"],
                            {"option": "delete",
                             "cluster": cluster_of_deleted_file,
                             "lines": need_to_update,
                             "table_name": table_name}
                        )
                        self.message_system.send(new_msg)

    def update_table(self, msg):
        table_name = msg["table_name"]
        is_teacher = msg["is_teacher"]
        rows = msg["rows"]
        file = pd.read_csv(table_name, sep=',')
        for row in rows:
            print(file[file.path == row["path"]])
            if file[file.path == row["path"]].empty:
                file.loc[len(file)] = row
            else:
                file[file.path == row["path"]] = row
            file.to_csv(table_name, index=False)
        if is_teacher:
            new_cluster = rows[0]["last_symbol"]
            for file_name, teacher in Watcher.OPENED_FILES:
                if not teacher:
                    file = pd.read_csv(file_name, sep=',')
                    for line in range(len(file)):
                        path = file.iloc[line, 0]
                        symbol = str(file.iloc[line, 1]).strip()
                        prev_cluster = str(file.iloc[line, 2]).strip()
                        if distance(symbol, prev_cluster) > distance(symbol, new_cluster):
                            file.cluster[file.path == path] = new_cluster
                    file.to_csv(file_name, index=False)


def distance(first, second):
    return abs(ord(first) - ord(second))
