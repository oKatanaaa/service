from threading import Thread

import pandas as pd

from messageSystem.Message import Message
from messageSystem.Utils import *
from picture_analyze_service.handlers.Watcher import Watcher


# noinspection PyMethodMayBeStatic
class TableHandler(Thread):

    def __init__(self, message_system):
        Thread.__init__(self)

        self.cls_size = 0

        self.setName("Table Handler")
        self.setDaemon(True)
        self.logger = None
        self.message_system = message_system
        self.address = message_system.ADDRESS_LIST[self.__class__.__name__]
        self.number_of_queue = len(message_system.queue_listing[self.address]) - 1

    def run(self):
        while True:
            msg = self.message_system.queue_listing[self.address][0].get()

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
                if msg[is_teacher]:
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
                print("Unknown operation")

    def __lock(self):
        msg = self.message_system.queue_listing[self.message_system.ADDRESS_LIST["Interrupt"]][0].get()
        self.update_table(msg)
        while not self.message_system.queue_listing[self.message_system.ADDRESS_LIST["Interrupt"]][0].empty:
            msg = self.message_system.queue_listing[self.message_system.ADDRESS_LIST["Interrupt"]][0].get()
            self.update_table(msg)

    def create_teacher_table(self, msg):
        name_of_table = msg[table_name]
        images = msg[files_with_content]
        file = pd.DataFrame({"path": [], "RGB": [], "cluster": []})
        file.columns = ['path', 'RGB', 'cluster']
        for image in images:
            file.loc[len(file)] = image
        file.to_csv(name_of_table, index=False)

    def create_table(self, msg):
        name_of_table = msg[table_name]
        images = msg[files_with_content]
        file = pd.DataFrame({"path": [], "RGB": [], "cluster": []})
        file.columns = ['path', 'RGB', 'cluster']
        for image in images:
            file.loc[len(file)] = image
        file.to_csv(name_of_table, index=False)

    def rename(self, msg):
        name_of_table = msg[table_name]
        old_file_name = msg[old_name]
        new_file_name = msg[new_name]
        file = pd.read_csv(name_of_table, sep=',')
        file.path[file.path == old_file_name] = new_file_name
        file.to_csv(name_of_table, index=False)

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
            try:
                cluster_of_deleted_file = file.RGB[file.path == target_path].iloc[0]
            except Exception:
                new_msg = Message(
                    self.address,
                    self.message_system.ADDRESS_LIST["Interrupt"],
                    {option: update_table_option,
                     files_with_content: [],
                     table_name: None,
                     is_teacher: None}
                )
                self.message_system.send(new_msg)
                return

            is_need_to_make_changes = len(file.RGB[file.RGB == cluster_of_deleted_file]) == 1
            file = file[file.path != target_path]
            file.to_csv(name_of_table, index=False)

            self.cls_size += 1

            if is_need_to_make_changes:

                new_msg = Message(
                    self.address,
                    self.message_system.ADDRESS_LIST["ClusterHandler"],
                    {option: delete_from_clusters_option,
                     deleted_cluster: cluster_of_deleted_file}
                )
                self.message_system.send(new_msg)

                for table in Watcher.OPENED_TABLES:
                    name_of_table = table[table_name]
                    teacher = table[is_teacher]
                    if not teacher:
                        table = pd.read_csv(name_of_table, sep=',')
                        try:
                            need_to_update = table[table.cluster == cluster_of_deleted_file]
                        except TypeError:
                            return

                        need_to_update = need_to_update.to_dict()
                        keys = need_to_update['path'].keys()
                        lines = []
                        for key in keys:
                            lines.append({'path': need_to_update['path'][key],
                                          'RGB': need_to_update['RGB'][key],
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
                            new_msg = Message(
                                self.address,
                                self.message_system.ADDRESS_LIST["Interrupt"],
                                {option: update_table_option,
                                 files_with_content: [],
                                 table_name: None,
                                 is_teacher: None}
                            )
                            self.message_system.send(new_msg)

            else:
                new_msg = Message(
                    self.address,
                    self.message_system.ADDRESS_LIST["Interrupt"],
                    {option: update_table_option,
                     files_with_content: [],
                     table_name: None,
                     is_teacher: None}
                )
                self.message_system.send(new_msg)

    def update_table(self, msg):
        name_of_table = msg.get(table_name)
        if name_of_table is None:
            return
        images = msg[files_with_content]

        if len(images) == 0:
            return

        file = pd.read_csv(name_of_table, sep=',')
        for row in images:
            if file[file.path == row["path"]].empty:
                file.loc[len(file)] = row
            else:
                file.path[file["path"] == row["path"]] = row['path']
                file.RGB[file['path'] == row['path']] = row['RGB']
                file.cluster[file['path'] == row['path']] = row['cluster']
        file.to_csv(name_of_table, index=False)

    def prepare_to_update(self, msg):
        name_of_table = msg[table_name]
        images = msg[files_with_content]

        if len(images) == 0:
            return

        for image in images:
            new_msg = {
                path: image["path"],
                table_name: name_of_table,
                is_teacher: True
            }
            self.delete(new_msg)

        new_msg = {
            table_name: name_of_table,
            is_teacher: True,
            files_with_content: images
        }
        self.update_table(new_msg)

        new_msg = Message(
            self.address,
            self.message_system.ADDRESS_LIST["ClusterHandler"],
            {option: create_clusters_option,
             files_with_content: images,
             'backtrack': True}
        )
        self.message_system.send(new_msg)

        msg = self.message_system.queue_listing[self.message_system.ADDRESS_LIST["Interrupt"]][0].get()
        while True:
            if msg.get(option) == update_table_option:
                self.update_table(msg)
                msg = self.message_system.queue_listing[self.message_system.ADDRESS_LIST["Interrupt"]][0].get()
            else:
                break
        clusters = None
        if msg.get(clusters_list) is not None:
            clusters = msg[clusters_list]

        new_cls = images[0]["RGB"]
        if len(images) > 1:
            print("So much clusters")
        for image in Watcher.OPENED_TABLES:
            file_name = image["table_name"]
            teacher = image["is_teacher"]

            if not teacher:
                images = pd.read_csv(file_name, sep=',')
                # if clusters is not None:
                #     images = images[images.cluster.isin(clusters)]
                images = images.to_dict(orient='records')

                new_msg = Message(
                    self.address,
                    self.message_system.ADDRESS_LIST["ClusterHandler"],
                    {option: soft_update_option,
                     table_name: file_name,
                     is_teacher: False,
                     'new_cluster': new_cls,
                     files_with_content: images}
                )

                self.message_system.send(new_msg)

