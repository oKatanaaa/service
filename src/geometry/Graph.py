from geometry import Point

"""
Класс представляющий собой некоторыйы граф (или его подобие)
@filed graph - point: set(point)
"""


class Graph:

    def __init__(self):
        self.graph = dict()

    def add_point(self, point: Point, neighbours: set):
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
