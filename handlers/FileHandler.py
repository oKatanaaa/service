import os
import sys
from threading import Thread

from messageSystem.Message import Message


# noinspection PyMethodMayBeStatic
class FileHandler(Thread):

    def __init__(self, message_system):
        Thread.__init__(self)
        self.setName("File Handler Daemon Thread")
        self.setDaemon(True)
        self.temp = None
        self.logger = None
        self.message_system = message_system
        self.address = message_system.ADDRESS_LIST[self.__class__.__name__]
        self.number_of_queue = len(message_system.queue_listing[self.address]) - 1

    def run(self):
        while True:
            msg = self.message_system.queue_listing[self.address][0].get()

            if msg["option"] == "first_generation":
                self.first_generation(msg)
            elif msg["option"] == "changes":
                self.get_changes(msg)

    def first_generation(self, msg):
        file_name_list = []
        target_path = msg["path"]
        for root_dir, dirs, files in os.walk(target_path):
            for file in files:
                if file[-4:] == ".txt":
                    file_name_list.append(os.path.join(root_dir, file))

        file_contents = self.grub_file_content(file_name_list)
        if msg["is_teacher"]:
            new_msg = Message(
                self.address,
                self.message_system.ADDRESS_LIST["ClusterHandler"],
                {"option": "create_clusters",
                 "table_name": msg["table_name"],
                 "file_contents": file_contents}
            )
            additional_msg = Message(
                self.address,
                self.message_system.ADDRESS_LIST["TableHandler"],
                {"option": "create_teacher_table",
                 "table_name": msg["table_name"],
                 "file_contents": file_contents}
            )
            self.message_system.send(additional_msg)
        else:
            new_msg = Message(
                self.address,
                self.message_system.ADDRESS_LIST["ClusterHandler"],
                {"option": "identify_clusters",
                 "table_name": msg["table_name"],
                 "file_contents": file_contents}
            )
        self.message_system.send(new_msg)

    def get_changes(self, msg):
        target_path = msg["target_path"]
        changes_list = msg["changes"]
        is_teacher = msg["is_teacher"]
        for action, file in changes_list:
            if file[-4:] != ".txt":
                continue

            full_filename = os.path.join(target_path, file)
            if action == 1:
                print("Created empty file. skip")

            elif action == 2:
                new_msg = Message(
                    self.address,
                    self.message_system.ADDRESS_LIST["TableHandler"],
                    {"option": "delete",
                     "is_teacher": is_teacher,
                     "table_name": msg["table_name"],
                     "path": full_filename}
                )
                self.message_system.send(new_msg)

            elif action == 3:
                if os.path.getsize(full_filename) != 0:
                    new_msg = Message(
                        self.address,
                        self.message_system.ADDRESS_LIST["ClusterHandler"],
                        {"option": "update",
                         "is_teacher": is_teacher,
                         "table_name": msg["table_name"],
                         "files": self.grub_file_content([full_filename])}
                    )
                    self.message_system.send(new_msg)

            elif action == 4:
                self.temp = full_filename

            elif action == 5:
                if sys.getsizeof(full_filename) != 0:
                    new_msg = Message(
                        self.address,
                        self.message_system.ADDRESS_LIST["TableHandler"],
                        {"option": "rename",
                         "table_name": msg["table_name"],
                         "old_name": self.temp,
                         "new_name": full_filename}
                    )
                    self.message_system.send(new_msg)

    def grub_file_content(self, file_name_list):
        file_content = []
        for name in file_name_list:

            try:
                with open(name, 'rb') as file:
                    file.seek(1, os.SEEK_END)
                    while file.tell() > 1 and file.seek(-3, os.SEEK_CUR):
                        ch = file.read(2).decode()[-1]

                        if ch.isalpha():
                            file_content.append({
                                "path": name,
                                "last_symbol": ch
                            })
                            break

            except UnicodeDecodeError:
                print("Logger catch UnicodeDecodeError")
            except PermissionError:
                print("File have used")
            except OSError:
                with open(name, 'rb') as file:
                    ch = file.read(1).decode()
                    if ch.isalpha():
                        file_content.append({
                            "path": name,
                            "last_symbol": ch
                        })
        return file_content
