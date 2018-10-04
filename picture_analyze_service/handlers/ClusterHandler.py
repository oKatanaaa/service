from threading import Thread

from messageSystem.Message import Message
from messageSystem.Utils import *
from picture_analyze_service.handlers.Cluster import Cluster


def distance(first, second):
    first = first.split(" ")
    R1 = int(first[0])
    G1 = int(first[1])
    B1 = int(first[2])
    second = second.split(" ")
    R2 = int(second[0])
    G2 = int(second[1])
    B2 = int(second[2])
    return pow((R1 - R2) * (R1 - R2) + (G1 - G2) * (G1 - G2) + (B1 - B2) * (B1 - B2), 0.5)


class ClusterHandler(Thread):

    def __init__(self, message_system, delete_type):
        Thread.__init__(self)
        self.setName("Cluster Handler")
        self.setDaemon(True)
        self.last_used_cluster = None
        self.trash = None
        self.files_count = 0
        self.clusters = dict()
        self.message_system = message_system
        self.address = message_system.ADDRESS_LIST[self.__class__.__name__]

        if delete_type == "old":
            self.delete = self.old_delete
        elif delete_type == "new":
            self.delete = self.new_delete
        else:
            print("Unknown option")

    def run(self):
        check_set = [10, 20, 30]
        while True:
            msg = self.message_system.queue_listing[self.address][0].get()

            if msg[option] == create_clusters_option:
                self.create_clusters(msg)
                cls_size = len(self.clusters.keys())
                if cls_size in check_set:
                    check_set.remove(cls_size)
                    new_msg = Message(
                        self.address,
                        self.message_system.ADDRESS_LIST['Utility'],
                        {}
                    )
                    self.message_system.send(new_msg)

            elif msg[option] == identify_clusters_option:
                self.identify_clusters(msg)

            elif msg[option] == hard_update_option:
                self.hard_update(msg)
                # TODO параметр
                if self.files_count == 30:
                    new_msg = Message(
                        self.address,
                        self.message_system.ADDRESS_LIST['Utility'],
                        {}
                    )
                    self.message_system.send(new_msg)

            elif msg[option] == delete_from_clusters_option:
                self.delete(msg)

            elif msg[option] == soft_update_option:
                self.soft_update(msg)

            elif msg[option] == update_after_delete:
                self.delete_update(msg)

            elif msg[option] == "kill":
                break

            else:
                print("Unknown option")

    def identify_clusters(self, msg):
        lines = msg[files_with_content]
        for line in lines:
            line["cluster"] = self.go_in_right_cluster(line["RGB"])
        new_msg = Message(
            self.address,
            self.message_system.ADDRESS_LIST["TableHandler"],
            {option: create_table_option,
             table_name: msg[table_name],
             files_with_content: lines}
        )
        self.message_system.send(new_msg)

    def create_clusters(self, msg):
        lines = msg[files_with_content]
        for line in lines:
            self.add_new_cluster(line["RGB"])
            if msg.get('backtrack') is not None:
                new_msg = Message(
                    self.address,
                    self.message_system.ADDRESS_LIST["Interrupt"],
                    {clusters_list: self.clusters[line["RGB"]]}
                )
                self.message_system.send(new_msg)

    def add_new_cluster(self, new_cluster):
        if self.clusters.get(new_cluster) is None:
            if len(self.clusters) == 0:
                self.clusters[new_cluster] = set()
                return

            result = []
            cls_list = list(self.clusters.keys())
            for cls in cls_list:
                result.append(Cluster(cls, distance(cls, new_cluster)))

            result.sort(key=lambda x: x.dist)
            self.__add_cls(new_cluster, result)

    def __add_cls(self, new_cluster, cls_list):
        sl = set()
        self.clusters[new_cluster] = set()

        for cluster in range(len(cls_list)):
            add_flag = True
            dist = cls_list[cluster].dist
            for neighbor in sl:
                neigh_dist = distance(neighbor, cls_list[cluster].name)
                if dist >= neigh_dist:
                    add_flag = False
                    break
            if add_flag:
                # TODO check. Вдруг не нужно добавлять.
                neighbor_set = self.clusters[cls_list[cluster].name]
                dist = distance(cls_list[cluster].name, new_cluster)
                add_flag = True
                for ngh in neighbor_set:
                    neigh_dist = distance(ngh, new_cluster)
                    if dist >= neigh_dist:
                        add_flag = False
                        break
                if add_flag:
                    self.clusters[new_cluster].add(cls_list[cluster].name)
                    self.clusters[cls_list[cluster].name].add(new_cluster)
                    sl.add(cls_list[cluster].name)

        if len(sl) == 0:
            self.clusters[new_cluster].add(cls_list[0].name)
            self.clusters[cls_list[0].name].add(new_cluster)

    # def __add_cls(self, new_cluster, cls_list):
    #     nearest = cls_list[0].name
    #
    #     #TODO нужно ли всегда так делать?
    #     self.clusters[new_cluster] = set()
    #     self.clusters[new_cluster].add(nearest)
    #     self.clusters[nearest].add(new_cluster)
    #
    #     sl = set()
    #     sl.add(nearest)
    #
    #     for cluster in range(1, len(cls_list)):
    #         add_flag = True
    #         dist = cls_list[cluster].dist
    #         for neighbor in sl:
    #             neigh_dist = distance(neighbor, cls_list[cluster].name)
    #             if dist >= neigh_dist:
    #                 add_flag = False
    #                 break
    #
    #         # if add_flag:
    #         #     self.clusters[new_cluster].add(cls_list[cluster].name)
    #         #     self.clusters[cls_list[cluster].name].add(new_cluster)
    #         #     sl.add(cls_list[cluster].name)
    #
    #         if add_flag:
    #             # TODO check. Вдруг не нужно добавлять.
    #             neighbor_set = self.clusters[cls_list[cluster].name]
    #             dist = distance(cls_list[cluster].name, new_cluster)
    #             add_flag = True
    #             for ngh in neighbor_set:
    #                 neigh_dist = distance(ngh, new_cluster)
    #                 if dist >= neigh_dist:
    #                     add_flag = False
    #                     break
    #             if add_flag:
    #                 self.clusters[new_cluster].add(cls_list[cluster].name)
    #                 self.clusters[cls_list[cluster].name].add(new_cluster)
    #                 sl.add(cls_list[cluster].name)
    #
    #     for cluster in sl:
    #         neighbor_set = set(self.clusters[cluster])
    #         for neighbor in neighbor_set:
    #             if distance(new_cluster, cluster) < distance(cluster, neighbor) and \
    #                     distance(new_cluster, neighbor) < distance(cluster, neighbor):
    #                 self.clusters[new_cluster].add(neighbor)
    #                 self.clusters[cluster].remove(neighbor)
    #                 self.clusters[neighbor].remove(cluster)
    #                 self.clusters[neighbor].add(new_cluster)

    def go_in_right_cluster(self, new_cluster):
        if self.last_used_cluster is not None:
            current = self.last_used_cluster
        else:
            current = list(self.clusters.keys())[0]
        hash_set = set(self.clusters.keys())
        hash_set.remove(current)

        while True:
            cluster = self.clusters[current]
            minimal = distance(current, new_cluster)
            the_smallest = float('inf')
            temp = None

            for neighbor in cluster:
                if neighbor in hash_set:
                    hash_set.remove(neighbor)
                    dif = distance(neighbor, new_cluster)
                    if dif < the_smallest:
                        the_smallest = dif
                        temp = neighbor

            if the_smallest > minimal:
                self.last_used_cluster = current
                return current
            elif len(hash_set) == 0:
                self.last_used_cluster = temp
                return temp
            else:
                current = temp

    def soft_update(self, msg):
        files = msg[files_with_content]
        new_cluster = msg['new_cluster']
        for file in files:
            file['cluster'] = self.go_in_right_cluster(file['RGB'])
            if file['cluster'] in self.clusters[new_cluster]:
                if distance(file["RGB"], file["cluster"]) > distance(file["RGB"], new_cluster):
                    file["cluster"] = new_cluster

        new_msg = Message(
            self.address,
            self.message_system.ADDRESS_LIST["Interrupt"],
            {option: update_table_option,
             table_name: msg[table_name],
             is_teacher: msg[is_teacher],
             files_with_content: files}
        )
        self.message_system.send(new_msg)

    """
    Дублированный метод (identify_cluster) для определения классификатора абсолтно новго файла
    """

    def hard_update(self, msg):
        files = msg[files_with_content]

        self.files_count += len(files)

        for file in files:
            file["cluster"] = self.go_in_right_cluster(file["RGB"])

        new_msg = Message(
            self.address,
            self.message_system.ADDRESS_LIST["TableHandler"],
            {option: update_table_option,
             table_name: msg[table_name],
             is_teacher: msg[is_teacher],
             files_with_content: files}
        )

        self.message_system.send(new_msg)

    """
    Метод обеспечивающий обновление кластеров после удаления одного из них.
    """

    def delete_update(self, msg):
        files = msg[files_with_content]
        cluster = msg[deleted_cluster]
        neighbors = self.trash[cluster]

        for file in files:
            _min = float('inf')
            answer = None
            for ngh in neighbors:
                dist = distance(ngh, file["RGB"])
                if dist < _min:
                    _min = dist
                    answer = ngh
            file["cluster"] = answer

        new_msg = Message(
            self.address,
            self.message_system.ADDRESS_LIST['Interrupt'],
            {option: update_table_option,
             files_with_content: files,
             table_name: msg[table_name],
             is_teacher: msg[is_teacher]}
        )
        self.message_system.send(new_msg)

    """
    Реализует удаление кластера.
    Сначала мы удаляем этот кластер из списков соседних
    А затем и сам этот кластер из списка
    Далее обновляем записи таблицы
    """

    def new_delete(self, msg):
        cluster = msg[deleted_cluster]
        self.trash = {cluster: self.clusters[cluster]}

        if len(self.clusters.keys()) == 2:
            self.clusters.pop(cluster, None)
            self.clusters[list(self.clusters.keys())[0]].discard(cluster)
        elif len(self.clusters.keys()) == 1:
            self.clusters.pop(cluster, None)
        self.last_used_cluster = None
        if cluster in self.clusters:
            have_deleted_cluster = self.clusters[cluster]
            self.clusters.pop(cluster, None)
            for neighbor in have_deleted_cluster:
                if neighbor in self.clusters:
                    self.clusters[neighbor].discard(cluster)
                    neighbors_2 = self.clusters[neighbor]
                    for ngh in have_deleted_cluster:
                        if ngh != neighbor:
                            is_crossing = True
                            for ngh_2 in neighbors_2:
                                if distance(ngh_2, ngh) <= distance(neighbor, ngh_2):
                                    is_crossing = False
                            if is_crossing:
                                self.clusters[neighbor].add(ngh)

    def old_delete(self, msg):
        cluster = msg[deleted_cluster]
        self.trash = {cluster: self.clusters[cluster]}

        if len(self.clusters.keys()) == 2:
            self.clusters.pop(cluster, None)
            self.clusters[list(self.clusters.keys())[0]].discard(cluster)
        elif len(self.clusters.keys()) == 1:
            self.clusters.pop(cluster, None)
        if cluster in self.clusters:
            self.last_used_cluster = None
            have_deleted_cluster = self.clusters[cluster]
            self.clusters.pop(cluster, None)
            for neighbor in have_deleted_cluster:
                for neighbor_2 in self.clusters[neighbor]:
                    if neighbor_2 != neighbor and neighbor_2 != cluster:
                        self.clusters[neighbor_2].remove(neighbor)
                self.clusters.pop(neighbor, None)
                self.add_new_cluster(neighbor)
