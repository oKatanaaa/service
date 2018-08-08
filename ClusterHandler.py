import logging
import time

import pandas as pd

"""
Класс предназначенный для работы с кластерами
"""


class ClusterHandler:
    clusters = dict()

    def __init__(self):
        """
        Инициализация объекта, через который мы будем работать с таблицей кластеров
        :param name - путь до таблицы, из которой будем формировать кластеры:
        """
        self.table_name = "teach_table.csv"
        self.logger = self.__get_logger("ClusterHandler")

    """
    
    """

    def go_in_right_cluster(self, last_symbol):
        while len(ClusterHandler.clusters) == 0:
            time.sleep(0.0001)
            pass
        hash_set = set(ClusterHandler.clusters.keys())
        current = list(ClusterHandler.clusters.keys())[0]
        hash_set.remove(current)
        while True:
            cluster = ClusterHandler.clusters[current]
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

    def add_new_cluster(self, new_cluster):
        if ClusterHandler.clusters.get(new_cluster) is None:
            keys = list(ClusterHandler.clusters.keys())
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
            ClusterHandler.clusters[new_cluster] = list(target)
            for cls in target:
                for neighbor in ClusterHandler.clusters[cls]:
                    if distance(new_cluster, cls) < distance(cls, neighbor) and \
                            distance(new_cluster, neighbor) < distance(cls, neighbor):
                        # Возможно стоит из-за этого использовать SET но я не уверен
                        ClusterHandler.clusters[new_cluster].append(neighbor)
                        ClusterHandler.clusters[cls].remove(neighbor)
                        ClusterHandler.clusters[neighbor].remove(cls)
                        ClusterHandler.clusters[neighbor].append(new_cluster)
                ClusterHandler.clusters[cls].append(new_cluster)

            self.update_results(new_cluster)
        pass

    def update_results(self, new_cluster):
        try:
            file = pd.read_csv(self.table_name, sep=',')
            for line in range(len(file)):
                path = file.iloc[line, 0]
                symbol = str(file.iloc[line, 1]).strip()
                prev_cluster = str(file.iloc[line, 2]).strip()
                if distance(symbol, prev_cluster) > distance(symbol, new_cluster):
                    file.cluster[file.path == path] = new_cluster
            file.to_csv(self.table_name, index=False)
        except UnicodeDecodeError:
            print("Catch UnicodeDecodeError")
        pass

    def __get_logger(self, logger_name: str):
        logger = logging.getLogger("Watcher " + logger_name)
        logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler(".\logs\my_watch.log")
        fh.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'))
        logger.addHandler(fh)
        return logger


def distance(first, second):
    return abs(ord(first) - ord(second))


if __name__ == "__main__":
    ch = ClusterHandler()
    print(ord('a'))
    print(ord('A'))
    print(ord('б'))
    print(ord('Б'))
    print(ord('я'))
    print(ord('Я'))
    ch.add_new_cluster('a')
    ch.add_new_cluster('z')
    ch.add_new_cluster('б')
    ch.add_new_cluster('я')
    print(ch.go_in_right_cluster('p'))
    ch.update_results('o')
