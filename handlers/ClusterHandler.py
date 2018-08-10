import logging
from threading import Thread

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

            if msg["option"] == "create_clusters":
                self.create_clusters(msg)
            elif msg["option"] == "identify_clusters":
                self.identify_clusters(msg)
            elif msg["option"] == "update":
                self.update(msg)
            elif msg["option"] == "delete":
                self.delete(msg)
            else:
                print("Unknown option")

    """
    Определение кластера для каждой переданной строки
    """
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
        self.logger.info("Cluster identify success")
        self.message_system.send(new_msg)

    """
    Добавление новых кластеров
    """
    def create_clusters(self, msg):
        lines = msg["file_contents"]
        for line in lines:
            self.add_new_cluster(line["last_symbol"])

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
            self.logger.info("Cluster - " + new_cluster + " was created")

    """
    Определение кластера по последнему символу.
    Мы берём случайный (пока что) кластер и проверяем расстояние от него и его соседей до целевого символа.
    Если минимальным расстоянием будет расстояние до кластера - то алгоритм заканчивает работу. Если
    ближе всего оказался сосед, то тебе берём этого соседа и его соседей, и повторяем алгоритм. 
    Алгоритм не проверяет уже проверенные кластеры
    """
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
                self.logger.info(last_symbol + " refers to " + current)
                return current
            else:
                current = temp

    """
    Если призошло какое-либо обновление данных файла, то будет вызван этот метод
    и в зависимости от того был ли это обучающий файл - будут предприняты меры.
    """
    def update(self, msg):
        is_teacher = msg["is_teacher"]
        files = msg["files"]
        for file in files:
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
             "rows": files}
        )
        self.logger.info("Clusters was up to date")
        self.message_system.send(new_msg)

    """
    Реализует удаление кластера.
    Сначала мы удаляем этот кластер из списков соседних
    А затем и сам этот кластер из списка
    Далее обновляем записи таблицы
    """
    def delete(self, msg):
        cluster_list = msg["cluster"]
        files = msg["files"]
        table_name = msg["table_name"]
        is_teacher = msg["is_teacher"]
        for cluster in cluster_list:
            if cluster in self.clusters:
                have_deleted_cluster = self.clusters[cluster]
                self.clusters.pop(cluster, None)
                for neighbor in have_deleted_cluster:
                    self.clusters.pop(neighbor, None)
                    self.add_new_cluster(neighbor)
        self.logger.info("DESTROY")
        self.update({
            "table_name": table_name,
            "files": files,
            "is_teacher": is_teacher,
            "is_deleted": True
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
