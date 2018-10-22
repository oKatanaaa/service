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
            self.graph.add_point(point, neighbours)
            self.__check_relationship(neighbours)
        else:
            self.graph.add_point(point, neighbours)
            self.__check_relationship(neighbours)

        pass

    def __check_relationship(self, point_set: set):
        for point in point_set:
            test_set = set()
            new_check_set = set()
            neighbours = self.graph.get_distance_to(point)
            test_set.add(neighbours[1][1])
            for index in range(2, len(neighbours)):
                add_flag = True
                for test_point in test_set:
                    if neighbours[index][0] > test_point.distance_to(neighbours[index][1]):
                        add_flag = False
                        new_check_set.add(neighbours[index][1])
                        break
                if add_flag:
                    test_set.add(neighbours[index][1])

            if len(new_check_set) != 0:
                self.graph.set_neighbours(point, test_set)
                # self.__check_relationship(new_check_set)

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
        temp = None
        minimal = point.distance_to(current)
        cluster_set = {x for x in self.graph.get_points()}
        cluster_set.discard(current)
        while True:
            the_smallest = float('inf')
            neighbours = self.graph.get_neighbours(current)
            cluster_set.difference_update(neighbours)
            for neighbour in neighbours:
                dist = neighbour.distance_to(point)
                if dist < the_smallest:
                    the_smallest = dist
                    temp = neighbour
            # noinspection PyChainedComparisons
            if the_smallest < minimal and len(cluster_set) != 0 and minimal != 0:
                minimal = the_smallest
                current = temp
            else:
                break
        self.cache = current
        return current
        pass
