import os
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
        for ngh in neighbours:
            self.graph.get(ngh).add(point)
        self.graph[point] = neighbours

    """
        Возвращает список изолированных точек
    """
    def remove_point(self, point: Point):
        isolated_points = []
        deleted_point_nghs = self.graph.get(point)
        for p in deleted_point_nghs:
            if self.is_in_graph(p):
                self.graph.get(p).discard(point)
            if len(self.graph.get(p)) == 0:
                isolated_points.append(p)
        self.graph.pop(point)
        return isolated_points

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
        return point.to_str() in self.graph

    def delete_neighbours(self, point: Point, deleted):
        if isinstance(deleted, Point):
            self.graph.get(point).discard(deleted)
        else:
            self.graph.get(point).difference_update({x for x in deleted})
    """
        Danger operation! Change current neighbours set to custom.
        :param point:
        :param neighbours: Set of new neighbours
        :return: none
    """
    def set_neighbours(self, point: Point, neighbours):
        if not isinstance(neighbours, set):
            neighbours = {neighbours}
        self.graph[point] = neighbours

    """
        This function returns a list of neighbours in tuples
        List format: [(tuple),(tuple),...]
        Tuple format: (distance to neighbour, neighbour point)
        Values in the list must be sorted by distances from nearest to farthest
    """
    def get_neighbours_list(self, point: Point):
        neighbours = self.get_neighbours(point)
        result_list = self.__point_to_points_dist(point, neighbours)
        return copy(result_list)

    """
        This function returns a list of each graph point in tuples
        List format: [(tuple),(tuple),...]
        Tuple format: (distance to point, point)
        Values in the list must be sorted by distances from nearest to farthest
    """
    def get_distance_to(self, point: Point):
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

    def __output_for_gmsh__(self, name: str):
        name = os.path.join(r'Q:\service\output', name)
        with open(name, 'w') as gmsh_file:
            lc = 'lc = 5e-2;\n'
            var_lc = 'lc'
            points = self.get_points()
            gmsh_file.write(lc)
            for index in range(len(points)):
                coordinates = points[index].to_str()
                coordinates = coordinates.replace(' ', ', ')
                gmsh_file.write("Point(" + str(index) + ") = {" + coordinates + ', ' + var_lc + '};\n')
            i = 1
            for index in range(len(points)):
                neighbours = self.get_neighbours(points[index])
                for ngh in neighbours:
                    ngh_index = points.index(ngh)
                    gmsh_file.write("Line(" + str(i) + ") = {" + str(index) + ',' + str(ngh_index) + "};\n")
                    i += 1
            pass
