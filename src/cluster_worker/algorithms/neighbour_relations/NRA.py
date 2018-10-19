from cluster_worker.algorithms.abstractAlgorithm.Algorithm import Algorithm
from geometry.Graph import Graph
from geometry.Point import Point


class NRA(Algorithm):

    # noinspection PyMissingConstructor
    def __init__(self):
        self.graph = Graph()
        pass

    def add_point(self, point: Point):
        if len(self.graph) == 0:
            self.graph.add_point(point)
            return
        distances = self.graph.get_distance_to(point)
        neighbours = set()

        for index in range(len(distances)):
            add_flag = True
            verifiable_cluster = distances[index][1]
            dist = distances[index][0]
            for neighbour in neighbours:
                dist_to_ngh = neighbour.distance_to(verifiable_cluster)
                if dist >= dist_to_ngh:
                    add_flag = False
                    break

            if add_flag:
                nghs_of_verify_cls = self.graph.get_neighbours_list(point)
                dist = verifiable_cluster.distance_to(point)
                for ngh in nghs_of_verify_cls:
                    dist_to_ngh = ngh[0]
                    if dist >= dist_to_ngh:
                        add_flag = False
                        break
                if add_flag:
                    self.graph.add_neighbours(verifiable_cluster, point)
                    neighbours.add(verifiable_cluster)

        if len(neighbours) == 0:
            neighbours.add(distances[0][1])
            self.graph.add_point(point, neighbours)
        else:
            self.graph.add_point(point, neighbours)

        self.graph.add_point(point, set())
        pass

    def delete_point(self, point: Point):
        if len(self.graph) < 3:
            self.graph.remove_point(point)
        elif self.graph.is_in_graph(point):


        pass

    def find_nearest_to(self, point: Point):
        clusters = list(self.graph.graph.keys())
        dif = float('inf')
        nearest = None
        for cluster in clusters:
            cluster = Point(cluster.split(' '))
            dist = point.distance_to(cluster)
            if dist < dif:
                dif = dist
                nearest = cluster
        return nearest
        pass