import time


class ClusterHandler:

    def __init__(self, name: str):
        self.table_name = name
        self.distances = list()
        self.clusters = dict()

    # def make_cluster(self):
    #     file = pd.read_csv(self.table_name, sep=',')
    #     clusters_list = file["last_symbol"]
    #     for cls in clusters_list:
    #         self.add_new_cluster(cls)
    #     # TODO: По одному кидать через add_new_cluster

    def go_in_right_cluster(self, last_symbol):
        while True and len(self.clusters) == 0:
            time.sleep(0.0001)
            pass
        current = list(self.clusters.keys())[0]
        while True:
            cluster = self.clusters[current]
            minimal = abs(ord(current) - ord(last_symbol))
            the_smallest = float('inf')
            temp = None
            for neighbor in cluster:
                dif = abs(ord(neighbor) - ord(last_symbol))
                if dif < the_smallest:
                    the_smallest = dif
                    temp = neighbor
            if the_smallest >= minimal:
                return current
            else:
                current = temp
            # TODO все работает, но предусматреть другой выход, если последний кластер оказался верным (if i == len?)

    def add_new_cluster(self, new_cluster):
        if len(self.clusters) == 0:
            self.clusters[new_cluster] = list()
        if self.clusters.get(new_cluster) is None:
            keys = list(self.clusters.keys())
            target = list()
            minim = float('inf')
            for key in keys:
                dif = abs(ord(key) - ord(new_cluster))
                if dif <= minim:
                    minim = dif
                    target.append(key)
            self.clusters[new_cluster] = list()
            for cls in target:
                self.clusters[cls].append(new_cluster)
                self.clusters[new_cluster].append(cls)
        pass

    def update_results(self, new_cluster):

        pass


if __name__ == "__main__":
    ch = ClusterHandler('name')
    ch.add_new_cluster('a')
    ch.add_new_cluster('z')
    ch.add_new_cluster('а')
    ch.add_new_cluster('я')
    print(ch.go_in_right_cluster('ц'))
