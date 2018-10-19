from geometry.Point import Point
from copy import copy

"""
Класс представляющий собой некоторыйы граф (или его подобие)
@filed graph - point: set(point)
"""


class Graph:

    def __init__(self):
        self.graph = dict()

    def add_point(self, point: Point, neighbours=None):
        if neighbours is None:
            neighbours = set()
        self.graph[point.to_str()] = neighbours

    def remove_point(self, point: Point):
        deleted_point_nghs = self.graph.get(point.to_str())
        for p in deleted_point_nghs:
            self.graph.get(p.to_str()).discard(point)
        self.graph.pop(point.to_str())

    def add_neighbours(self, point: Point, added):
        self.graph.get(point.to_str()).update(added)

    def get_neighbours(self, point: Point):
        return copy(self.graph.get(point.to_str()))

    def get_points(self):
        key_list = list(self.graph.keys())
        for index in range(len(key_list)):
            key_list[index] = Point(key_list[index].split(' '))
        return key_list

    def is_in_graph(self, point: Point):
        return point.to_str() in self.graph

    def set_neighbours(self, point: Point, neighbours: set):
        """
        Danger operation! Change current neighbours set to custom.
        :param point:
        :param neighbours: Set of new neighbours
        :return: none
        """
        self.graph[point.to_str()] = neighbours

    # NEED TO CREATE

    def get_neighbours_list(self, point: Point):
        """
        This function returns a list of neighbours in tuples
        List format: [(tuple),(tuple),...]
        Tuple format: (distance to neighbour, neighbour point)
        Values in the list must be sorted by distances from nearest to farthest
        """
        neighbours = self.get_neighbours(point)
        result_list = self.__point_to_points_dist(point, neighbours)
        return copy(result_list)

    def get_distance_to(self, point: Point):
        """
        This function returns a list of each graph point in tuples
        List format: [(tuple),(tuple),...]
        Tuple format: (distance to point, point)
        Values in the list must be sorted by distances from nearest to farthest
        """
        graph_points_list = self.get_points()
        result_list = self.__point_to_points_dist(point, graph_points_list)
        return result_list
        pass

    def to_str(self):
        data = ""
        for x in list(self.graph.keys()):
            data += x.to_str() + ": "
            for ngh in self.graph.get(x):
                data += ngh.to_str() + " "
            data += '\n'
        return data

    # noinspection PyMethodMayBeStatic
    def __point_to_points_dist(self, point: Point, points_list: list):
        result_list = []
        for p in points_list:
            result_list.append((point.distance_to(p), p))
        result_list.sort(key=lambda x: x[0])
        return result_list

    def __len__(self):
        return len(self.graph)
