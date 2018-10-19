from copy import copy

from geometry.Point import Point

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
        elif not isinstance(neighbours, set):
            neighbours = {neighbours}
        self.graph[point] = neighbours

    def remove_point(self, point: Point):
        deleted_point_nghs = self.graph.get(point)
        for p in deleted_point_nghs:
            self.graph.get(p).discard(point)
        self.graph.pop(point)

    def add_neighbours(self, point: Point, neighbours):
        if not isinstance(neighbours, set):
            neighbours = {neighbours}
        self.graph.get(point).update(neighbours)

    def get_neighbours(self, point: Point):
        return copy(self.graph.get(point))

    def get_points(self):
        key_list = list(self.graph.keys())
        return key_list

    def is_in_graph(self, point: Point):
        return point in self.graph

    def set_neighbours(self, point: Point, neighbours):
        """
        Danger operation! Change current neighbours set to custom.
        :param point:
        :param neighbours: Set of new neighbours
        :return: none
        """
        if not isinstance(neighbours, set):
            neighbours = {neighbours}
        self.graph[point] = neighbours

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
            data += x + ": "
            for ngh in self.graph.get(x):
                data += ngh + " "
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
