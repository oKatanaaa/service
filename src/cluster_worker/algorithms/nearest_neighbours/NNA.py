from cluster_worker.algorithms.abstractAlgorithm.Algorithm import Algorithm
from geometry.Graph import Graph
from geometry.Point import Point


class NNA(Algorithm):

    # noinspection PyMissingConstructor
    def __init__(self):
        self.graph = Graph()
        pass

    def add_point(self, point: Point):
        self.graph.add_point(point, set())
        pass

    def delete_point(self, point: Point):
        self.graph.remove_point(point)
        pass

    def find_nearest_to(self, point: Point):
        clusters = list(self.graph.graph.keys())
        dif = float('inf')
        nearest = None
        for cluster in clusters:
            dist = point.distance_to(cluster)
            if dist < dif:
                dif = dist
                nearest = cluster
        return nearest
        pass
