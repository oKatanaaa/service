from cluster_worker.algorithms.abstractAlgorithm.Algorithm import Algorithm
from geometry.Graph import Graph
from geometry.Point import Point


class NRA(Algorithm):

    # noinspection PyMissingConstructor
    def __init__(self):
        self.graph = Graph()
        self.cache = None
        pass

    def add_point(self, point: Point):
        if len(self.graph) == 0:
            self.cache = point
            self.graph.add_point(point)
            return
        elif len(self.graph) == 1:
            fp = self.graph.get_points()[0]
            self.graph.add_point(point, fp)
            self.graph.set_neighbours(fp, point)
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
                nghs_of_verify_cls = self.graph.get_neighbours_list(verifiable_cluster)
                dist = verifiable_cluster.distance_to(point)
                for ngh in nghs_of_verify_cls:
                    dist_to_ngh = ngh[0]
                    if dist >= dist_to_ngh:
                        add_flag = False
                        break
                if add_flag:
                    self.graph.add_neighbours(verifiable_cluster, {point})
                    neighbours.add(verifiable_cluster)

        if len(neighbours) == 0:
            neighbours.add(distances[0][1])
            self.graph.add_point(point, neighbours)
        else:
            self.graph.add_point(point, neighbours)
        pass

    def delete_point(self, point: Point):
        if len(self.graph) < 3:
            self.graph.remove_point(point)
        elif self.graph.is_in_graph(point):
            neighbours = self.graph.get_neighbours(point)
            self.graph.remove_point(point)
            for neighbour in neighbours:
                self.graph.remove_point(neighbour)
                self.add_point(neighbour)
        pass

    def find_nearest_to(self, point: Point):
        # TODO начинать с центра графа
        current = self.cache
        graph_points = self.graph.get_points()
        graph_points.remove(current)
        dist = point.distance_to(current)
        while True:
            neighbours = self.graph.get_neighbours_list(current)
            the_smallest = neighbours[0][0]
            if the_smallest < dist:
                dist = the_smallest
                current = neighbours[0][1]
                graph_points.remove(current)
            else:
                break
            if len(graph_points) == 0:
                break
        self.cache = current
        return current
        pass
