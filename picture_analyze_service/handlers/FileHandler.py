import os
import sys
from statistics import mean
from threading import Thread

from scipy import misc

from messageSystem.Message import Message
from messageSystem.Utils import *


def to_string(R, G, B):
    return str(str(R) + " " + str(G) + " " + str(B))


def open_images(image_list):
    images = []
    for image_name in image_list:
        image = misc.imread(image_name, False, 'RGB').flatten()
        length = len(image)
        R = [image[i] for i in range(length) if i % 3 == 0]
        G = [image[i] for i in range(length) if i % 3 == 1]
        B = [image[i] for i in range(length) if i % 3 == 2]
        images.append({"path": image_name,
                       "RGB": to_string(mean(R), mean(G), mean(B)),
                       "cluster": ""})
    return images


class FileHandler(Thread):
    ACTIONS = {
        1: "Created",
        2: "Deleted",
        3: "Updated",
        4: "Renamed from ",
        5: "Renamed to "
    }

    def __init__(self, message_system):
        Thread.__init__(self)
        self.setName("File Handler")
        self.setDaemon(True)
        self.temp = None
        self.message_system = message_system
        self.address = message_system.ADDRESS_LIST[self.__class__.__name__]
        self.number_of_queue = len(message_system.queue_listing[self.address]) - 1

    def run(self):
        while True:
            msg = self.message_system.queue_listing[self.address][0].get()

            if msg[option] == first_generation_option:
                self.first_generation(msg)
            elif msg[option] == changes_option:
                self.get_changes(msg)
            elif msg[option] == "kill":
                break
            else:
                print("Unknown option")

    def first_generation(self, msg):
        file_name_list = []
        target_path = msg[path]
        for root_dir, dirs, files in os.walk(target_path):
            for file in files:
                if file[-4:] == ".tif":
                    file_name_list.append(os.path.join(root_dir, file))
        images = open_images(file_name_list)
        if msg[is_teacher]:
            new_msg = Message(
                self.address,
                self.message_system.ADDRESS_LIST["TableHandler"],
                {option: create_teacher_tables_option,
                 table_name: msg[table_name],
                 files_with_content: images}
            )
            self.message_system.send(new_msg)

            new_msg = Message(
                self.address,
                self.message_system.ADDRESS_LIST["ClusterHandler"],
                {option: create_clusters_option,
                 table_name: msg[table_name],
                 files_with_content: images}
            )
            self.message_system.send(new_msg)

        else:
            new_msg = Message(
                self.address,
                self.message_system.ADDRESS_LIST["ClusterHandler"],
                {option: identify_clusters_option,
                 table_name: msg[table_name],
                 files_with_content: images}
            )
            self.message_system.send(new_msg)

    def get_changes(self, msg):
        target_path = msg[path]
        changes_list = msg[changes]
        teacher = msg[is_teacher]
        for action, file in changes_list:
            if file is None or file[-4:] != ".tif":
                continue

            full_filename = os.path.join(target_path, file)

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
                            {option: hard_update_option,
                             is_teacher: teacher,
                             table_name: msg[table_name],
                             files_with_content: open_images([full_filename])}
                        )
                    else:
                        new_msg = Message(
                            self.address,
                            self.message_system.ADDRESS_LIST["TableHandler"],
                            {option: prepare_to_update_option,
                             is_teacher: teacher,
                             table_name: msg[table_name],
                             files_with_content: open_images([full_filename])}
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
