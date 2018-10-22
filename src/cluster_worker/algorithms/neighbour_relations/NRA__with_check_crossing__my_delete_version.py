from copy import copy

from cluster_worker.algorithms.abstractAlgorithm.Algorithm import Algorithm
from geometry.Graph import Graph
from geometry.Point import Point

"""
    Стандартный алгоритм по работе с кластерами, с таким вариантом удаление: удаляем точки являющиеся соседями удаляемой
    и добавляем их обратно, в теории, таким образом граф должен перестраиваться правильно.
"""


class NRA(Algorithm):

    # noinspection PyMissingConstructor
    def __init__(self):
        self.graph = Graph()
        self.cache = None
        pass

    """
        Вычисляем расстояние от добавляемой точки до всех имеющихся точек.
        Добавляем ближайшую в список соседей
    """

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
                neighbours.add(verifiable_cluster)

        if len(neighbours) == 0:
            neighbours.add(distances[0][1])
            neighbours = self.__check_crossing(point, neighbours)
            self.graph.add_point(point, neighbours)
        else:
            neighbours = self.__check_crossing(point, neighbours)
            self.graph.add_point(point, neighbours)

        pass

    def __check_crossing(self, new_point, neighbours_of_new):
        for neighbour in copy(neighbours_of_new):
            neighbours_of_current = self.graph.get_neighbours(neighbour)
            for ngh_o_c in neighbours_of_current:
                if new_point.distance_to(neighbour) < ngh_o_c.distance_to(neighbour) and \
                        new_point.distance_to(ngh_o_c) < ngh_o_c.distance_to(neighbour):
                    neighbours_of_new.add(ngh_o_c)
                    self.graph.delete_neighbours(neighbour, ngh_o_c)
                    self.graph.delete_neighbours(ngh_o_c, neighbour)
        return neighbours_of_new

    def delete_point(self, point: Point):
        if len(self.graph) < 3:
            isolated_points = self.graph.remove_point(point)
            for p in isolated_points:
                self.graph.remove_point(p)
                self.add_point(p)
        elif self.graph.is_in_graph(point):
            neighbours = self.graph.get_neighbours(point)
            self.graph.remove_point(point)
            for neighbour in neighbours:
                isolated_points = self.graph.remove_point(neighbour)
                for p in isolated_points:
                    self.graph.remove_point(p)
                    self.add_point(p)
                self.add_point(neighbour)
        pass

    def find_nearest_to(self, point: Point):
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
