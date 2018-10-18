from geometry import Point

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
        self.graph[point] = neighbours

    def remove_point(self, point: Point):
        self.graph.pop(point)

    def remove_neighbours(self, point: Point, removable: set):
        self.graph.get(point).difference_update(removable)

    def add_neighbours(self, point: Point, added):
        self.graph.get(point).update(added)

    def get_neighbours(self, point: Point):
        return self.graph.get(point)

    def is_in_graph(self, point: Point):
        return point in self.graph

    def set_neighbours(self, point: Point, neighbours: set):
        """
        Danger operation! Change current neighbours set to custom.
        :param neighbours: Set of new neighbours
        :return: none
        """
        self.graph[point] = neighbours

    # NEED TO CREATE

    def get_neighbours_list(self, point: Point):
        """
        This function returns a list of neighbours in tuples
        List format: [(tuple),(tuple),...]
        Tuple format: (distance to neighbour, neighbour point)
        Values in the list must be sorted by distances from nearest to farthest
        """
        pass

    def get_distance_to(self, point: Point):
        """
        This function returns a list of each graph point in tuples
        List format: [(tuple),(tuple),...]
        Tuple format: (distance to point, point)
        Values in the list must be sorted by distances from nearest to farthest
        """
        pass
