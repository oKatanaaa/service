import pandas as pd


class ClusterBase:

    def __init__(self):
        self.clusters = None

    def make_cluster(self):
        file = pd.read_csv("teach_table.csv", sep=',')
        self.clusters = file["last_symbol"]

    def go_in_right_cluster(self, last_symbol):
        minimal = float('inf')
        right_cluster = None
        for cluster in self.clusters:
            dif = abs(ord(cluster) - ord(last_symbol))
            if dif < minimal:
                minimal = dif
                right_cluster = cluster
        return right_cluster
