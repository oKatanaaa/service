import time

import pandas as pd


class ClusterHandler:

    def __init__(self, name: str):
        self.table_name = name
        self.distances = list()
        self.clusters = dict()

    def go_in_right_cluster(self, last_symbol):
        while True and len(self.clusters) == 0:
            time.sleep(0.0001)
            pass
        current = list(self.clusters.keys())[0]
        while True:
            cluster = self.clusters[current]
            minimal = distance(current, last_symbol)
            the_smallest = float('inf')
            temp = None
            for neighbor in cluster:
                dif = distance(neighbor, last_symbol)
                if dif < the_smallest:
                    the_smallest = dif
                    temp = neighbor
            if the_smallest >= minimal:
                return current
            else:
                current = temp

    def add_new_cluster(self, new_cluster):
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
                else:
                    target.append(key)
            self.clusters[new_cluster] = list(target)
            for cls in target:
                for neighbor in self.clusters[cls]:
                    if distance(new_cluster, cls) < distance(cls, neighbor) and \
                            distance(new_cluster, neighbor) < distance(cls, neighbor):
                        self.clusters[cls].remove(neighbor)
                self.clusters[cls].append(new_cluster)

        pass

    def update_results(self, new_cluster):
        try:
            file = pd.read_csv(self.table_name, sep=',')
            for line in range(len(file)):
                path = file.iloc[line, 0]
                symbol = file.iloc[line, 1]
                prev_cluster = file.iloc[line, 2]
                if distance(symbol, prev_cluster) > distance(symbol, new_cluster):
                    file.cluster[file.path == path] = new_cluster
            file.to_csv(self.table_name, index=False)
        except UnicodeDecodeError:
            print("Catch UnicodeDecodeError")
        pass


def distance(first, second):
    return abs(ord(first) - ord(second))


if __name__ == "__main__":
    ch = ClusterHandler('name')
    ch.add_new_cluster('a')
    ch.add_new_cluster('z')
    ch.add_new_cluster('б')
    ch.add_new_cluster('я')
    print(ch.go_in_right_cluster('p'))
    ch.update_results('o')
