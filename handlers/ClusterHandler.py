from threading import Thread

from messageSystem.Message import Message


class ClusterHandler(Thread):

    def __init__(self, message_system):
        Thread.__init__(self)
        self.setName("Cluster Handler Daemon Thread")
        self.setDaemon(True)
        self.logger = None
        self.clusters = dict()
        self.message_system = message_system
        self.address = message_system.ADDRESS_LIST[self.__class__.__name__]
        self.number_of_queue = len(message_system.queue_listing[self.address]) - 1

    def run(self):
        while True:
            msg = self.message_system.queue_listing[self.address][0].get()

            if msg["option"] == "create_clusters":
                self.create_clusters(msg)
            elif msg["option"] == "identify_clusters":
                self.identify_clusters(msg)
            elif msg["option"] == "update":
                self.update(msg)
            elif msg["option" == "delete"]:
                self.delete(msg)

    def identify_clusters(self, msg):
        lines = msg["file_contents"]
        for line in lines:
            line["cluster"] = self.go_in_right_cluster(line["last_symbol"])
        new_msg = Message(
            self.address,
            self.message_system.ADDRESS_LIST["TableHandler"],
            {"option": "create_table",
             "table_name": msg["table_name"],
             "file_contents": lines}
        )
        self.message_system.send(new_msg)

    def create_clusters(self, msg):
        lines = msg["file_contents"]
        for line in lines:
            self.add_new_cluster(line["last_symbol"])

    def add_new_cluster(self, new_cluster):
        # new_cluster = msg["new_cluster"]
        if self.clusters.get(new_cluster) is None:
            keys = list(self.clusters.keys())
            target = list()
            minim = float('inf')
            for key in keys:
                dif = distance(key, new_cluster)
                if dif < minim:
                    minim = dif
                    target.clear()
                    target.append(key)
                elif dif == minim:
                    target.append(key)
            self.clusters[new_cluster] = list(target)
            for cls in target:
                for neighbor in self.clusters[cls]:
                    if distance(new_cluster, cls) < distance(cls, neighbor) and \
                            distance(new_cluster, neighbor) < distance(cls, neighbor):
                        # Возможно стоит из-за этого использовать SET но я не уверен
                        self.clusters[new_cluster].append(neighbor)
                        self.clusters[cls].remove(neighbor)
                        self.clusters[neighbor].remove(cls)
                        self.clusters[neighbor].append(new_cluster)
                self.clusters[cls].append(new_cluster)

    def go_in_right_cluster(self, last_symbol):
        hash_set = set(self.clusters.keys())
        current = list(self.clusters.keys())[0]
        hash_set.remove(current)
        while True:
            cluster = self.clusters[current]
            minimal = distance(current, last_symbol)
            the_smallest = float('inf')
            temp = None
            for neighbor in cluster:
                if neighbor in hash_set:
                    hash_set.remove(neighbor)
                    dif = distance(neighbor, last_symbol)
                    if dif < the_smallest:
                        the_smallest = dif
                        temp = neighbor
            if the_smallest > minimal or len(hash_set) == 0:
                return current
            else:
                current = temp

    def update(self, msg):
        is_teacher = msg["is_teacher"]
        files = msg["files"]
        if len(files) == 0:
            return
        file = msg["files"][0]
        if is_teacher:
            self.add_new_cluster(file["last_symbol"])
        else:
            file["cluster"] = self.go_in_right_cluster(file["last_symbol"])

        new_msg = Message(
            self.address,
            self.message_system.ADDRESS_LIST["TableHandler"],
            {"option": "update_table",
             "table_name": msg["table_name"],
             "is_teacher": is_teacher,
             "rows": [file]}
        )
        self.message_system.send(new_msg)

    def delete(self, msg):
        cluster = msg["cluster"]
        need_to_update = msg["lines"]
        table_name = msg["table_name"]
        have_deleted_cluster = self.clusters[cluster]
        for neighbor in have_deleted_cluster:
            self.clusters[neighbor].remove(cluster)
        self.clusters = self.clusters.pop[cluster, None]
        for line in need_to_update:
            line["cluster"] = self.go_in_right_cluster(line["last_symbol"])
        new_msg = Message(
            self.address,
            self.message_system.ADDRESS_LIST["TableHandler"],
            {"option": "update_table",
             "table_name": table_name,
             "rows": need_to_update,
             "is_teacher": False}
        )
        self.message_system.send(new_msg)


def distance(first, second):
    return abs(ord(first) - ord(second))
