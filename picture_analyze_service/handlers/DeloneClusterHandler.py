from threading import Thread

from messageSystem.Message import Message
from messageSystem.Utils import *
from picture_analyze_service.handlers.Cluster import Cluster
from picture_analyze_service.handlers.TriangulationDelone import TriangulationDelone


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
        self.delone = TriangulationDelone()
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
                cls_size = len(self.delone.clusters.keys())
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
                    {clusters_list: self.delone.clusters[line["RGB"]]}
                )
                self.message_system.send(new_msg)

    def add_new_cluster(self, new_cluster):
        self.delone.add_point(new_cluster)

    def go_in_right_cluster(self, new_cluster):
        if self.last_used_cluster is not None:
            current = self.last_used_cluster
        else:
            current = list(self.delone.clusters.keys())[0]
        hash_set = set(self.delone.clusters.keys())
        hash_set.remove(current)

        while True:
            cluster = self.delone.clusters[current]
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
            if file['cluster'] in self.delone.clusters[new_cluster]:
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
        self.trash = {cluster: self.delone.clusters[cluster]}

        self.last_used_cluster = None
        if cluster in self.delone.clusters:
            have_deleted_cluster = self.delone.clusters[cluster]
            self.delone.clusters.pop(cluster, None)
            for neighbor in have_deleted_cluster:
                if neighbor in self.delone.clusters:
                    self.delone.clusters[neighbor].discard(cluster)
                    neighbors_2 = self.delone.clusters[neighbor]
                    for ngh in have_deleted_cluster:
                        if ngh != neighbor:
                            is_crossing = True
                            for ngh_2 in neighbors_2:
                                if distance(ngh_2, ngh) <= distance(neighbor, ngh_2):
                                    is_crossing = False
                            if is_crossing:
                                self.delone.clusters[neighbor].add(ngh)

    def old_delete(self, msg):
        cluster = msg[deleted_cluster]
        self.trash = {cluster: self.delone.clusters[cluster]}

        if cluster in self.delone.clusters:
            self.last_used_cluster = None
            have_deleted_cluster = self.delone.clusters[cluster]
            self.delone.clusters.pop(cluster, None)
            for neighbor in have_deleted_cluster:
                for neighbor_2 in self.delone.clusters[neighbor]:
                    if neighbor_2 != neighbor and neighbor_2 != cluster:
                        self.delone.clusters[neighbor_2].remove(neighbor)
                self.delone.clusters.pop(neighbor, None)
                self.add_new_cluster(neighbor)
