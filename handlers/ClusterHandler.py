import logging
from threading import Thread

from handlers.Utils import *
from messageSystem.Message import Message

"""
Класс обработчик кластеров
Поток демон
"""


class ClusterHandler(Thread):
    # Todo: static filed with clusters

    def __init__(self, message_system):
        Thread.__init__(self)
        self.setName("Cluster Handler Daemon Thread")
        self.setDaemon(True)
        self.logger = self.__get_logger()
        self.clusters = dict()
        self.last_used_cluster = None
        """ Ссылка на общий для всех объект - системы сообщений     """
        self.message_system = message_system
        """ Адрес класса в системе сообщений """
        self.address = message_system.ADDRESS_LIST[self.__class__.__name__]
        self.number_of_queue = len(message_system.queue_listing[self.address]) - 1
        self.logger.info("Class " + self.__class__.__name__ + " successfully initialized")

    """
    Запуск обработчика
    """
    def run(self):
        while True:
            msg = self.message_system.queue_listing[self.address][0].get()
            self.logger.info(self.__class__.__name__ + " have a message")

            if msg[option] == create_clusters_option:
                self.create_clusters(msg)
            elif msg[option] == identify_clusters_option:
                self.identify_clusters(msg)
            elif msg[option] == update_cluster_option:
                self.update(msg)
            elif msg[option] == delete_from_clusters_option:
                self.delete(msg)
            elif msg[option] == soft_update_option:
                self.soft_update(msg)
            else:
                print("Unknown option")

    """
    Определение кластера для каждой переданной строки
    """
    def identify_clusters(self, msg):
        lines = msg[files_with_content]
        for line in lines:
            line["cluster"] = self.go_in_right_cluster(line[last_symbol])
        new_msg = Message(
            self.address,
            self.message_system.ADDRESS_LIST["TableHandler"],
            {option: create_table_option,
             table_name: msg[table_name],
             files_with_content: lines}
        )
        self.logger.info("Cluster identify success")
        self.message_system.send(new_msg)

    """
    Добавление новых кластеров
    """
    def create_clusters(self, msg):
        lines = msg[files_with_content]
        for line in lines:
            self.add_new_cluster(line[last_symbol])

    """
    Добавление кластера.
    Если нового кластера в таблице еще нет. То мы находим ближайший к нему кластер из уже имеющихся
    Они будут друг другу соседями. Далее проверяем. У каждого (их могло было быть несколько)
    ближайшего кластера осматриваем соседей. Если наш новый кластер, находится в пересечении областей
    образованных окружностями радиуса равного расстоянию от ближайшего кластера до его соседа. Грубо говоря находися 
    между своим ближайшим кластером и его соседом. То это означает что сосед теперь становится соседом для нашего
    нового кластера, и убирается из списка соседов соответсвенно.
    """
    def add_new_cluster(self, new_cluster):

        if self.clusters.get(new_cluster) is None:
            if len(self.clusters) == 0:
                self.clusters[new_cluster] = set()

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
                    self.logger.info(new_cluster + " refers to " + current)

                    self.__add_cls(new_cluster, current)

                    self.logger.info("Cluster - " + new_cluster + " was created")
                    return
                elif len(hash_set) == 0:
                    self.__add_cls(new_cluster, temp)
                    return
                else:
                    current = temp

    def __add_cls(self, new_cluster, current):
        self.clusters[new_cluster] = set(current)
        neighbor_set = set(self.clusters[current])
        for neighbor in neighbor_set:
            if distance(new_cluster, current) < distance(current, neighbor) and \
                    distance(new_cluster, neighbor) < distance(current, neighbor):
                self.clusters[new_cluster].add(neighbor)
                self.clusters[current].remove(neighbor)
                self.clusters[neighbor].remove(current)
                self.clusters[neighbor].add(new_cluster)
        self.clusters[current].add(new_cluster)


    """
    Определение кластера по последнему символу.
    Мы берём случайный (пока что) кластер и проверяем расстояние от него и его соседей до целевого символа.
    Если минимальным расстоянием будет расстояние до кластера - то алгоритм заканчивает работу. Если
    ближе всего оказался сосед, то тебе берём этого соседа и его соседей, и повторяем алгоритм. 
    Алгоритм не проверяет уже проверенные кластеры
    """

    def go_in_right_cluster(self, _last_symbol):
        if self.last_used_cluster is not None:
            current = self.last_used_cluster
        else:
            current = list(self.clusters.keys())[0]
        hash_set = set(self.clusters.keys())
        hash_set.remove(current)

        while True:
            cluster = self.clusters[current]
            minimal = distance(current, _last_symbol)
            the_smallest = float('inf')
            temp = None

            for neighbor in cluster:
                if neighbor in hash_set:
                    hash_set.remove(neighbor)
                    dif = distance(neighbor, _last_symbol)
                    if dif < the_smallest:
                        the_smallest = dif
                        temp = neighbor

            if the_smallest > minimal:
                self.logger.info(_last_symbol + " refers to " + current)
                self.last_used_cluster = current
                return current
            elif len(hash_set) == 0:
                self.last_used_cluster = temp
                return temp
            else:
                current = temp

    """
    Если призошло какое-либо обновление данных файла, то будет вызван этот метод
    и в зависимости от того был ли это обучающий файл - будут предприняты меры.
    """
    def update(self, msg):
        teacher = msg[is_teacher]
        files = msg[files_with_content]

        if not msg[is_deleted]:
            deleted_cls = None
        else:
            deleted_cls = msg[deleted_cluster]
        for file in files:
            if teacher:
                self.add_new_cluster(file[last_symbol])
            else:
                file["cluster"] = self.go_in_right_cluster(file[last_symbol])

        new_msg = Message(
            self.address,
            self.message_system.ADDRESS_LIST["TableHandler"],
            {option: update_table_option,
             table_name: msg[table_name],
             is_teacher: teacher,
             is_deleted: msg[is_deleted],
             deleted_cluster: deleted_cls,
             files_with_content: files}
        )
        self.logger.info("Clusters was up to date")
        self.message_system.send(new_msg)

    def soft_update(self, msg):
        files = msg[files_with_content]
        new_cluster = msg['new_cluster']

        for file in files:
            if distance(file['last_symbol'], file['cluster']) > distance(file['last_symbol'], new_cluster):
                file['cluster'] = new_cluster

        new_msg = Message(
            self.address,
            self.message_system.ADDRESS_LIST["TableHandler"],
            {option: update_table_option,
             table_name: msg[table_name],
             is_teacher: msg[is_teacher],
             is_deleted: msg[is_deleted],
             deleted_cluster: None,
             files_with_content: files}
        )
        self.message_system.send(new_msg)


    """
    Реализует удаление кластера.
    Сначала мы удаляем этот кластер из списков соседних
    А затем и сам этот кластер из списка
    Далее обновляем записи таблицы
    """
    def delete(self, msg):
        cluster_lst = msg[deleted_cluster]
        files = msg[files_with_content]
        teacher = msg[is_teacher]

        for cluster in cluster_lst:
            if self.last_used_cluster == cluster:
                self.last_used_cluster = None
            if cluster in self.clusters:
                have_deleted_cluster = self.clusters[cluster]
                self.clusters.pop(cluster, None)
                for neighbor in have_deleted_cluster:
                    for neighbor_2 in self.clusters[neighbor]:
                        if neighbor_2 != neighbor and neighbor_2 != cluster:
                            self.clusters[neighbor_2].remove(neighbor)
                    self.clusters.pop(neighbor, None)
                    self.add_new_cluster(neighbor)

        self.logger.info("DESTROY")
        self.update({
            table_name: msg[table_name],
            files_with_content: files,
            is_teacher: teacher,
            is_deleted: True,
            deleted_cluster: cluster_lst
        })

    def __get_logger(self):
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler(".\logs\my_watch.log")
        fh.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'))
        logger.addHandler(fh)
        return logger


def distance(first, second):
    return abs(ord(first) - ord(second))
