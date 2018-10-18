from cluster_worker.algorithms.abstractAlgorithm.Algorithm import Algorithm
from geometry.Graph import Graph
from geometry.Point import Point

class DVA(Algorithm):
    def __init__(self, graph: Graph, dist_multiplier = 1.0):
        if dist_multiplier < 1.0:
            raise Exception('dist_multiplier cannot be lower than 1.0')

        self.graph = graph
        self.dist_multiplier = dist_multiplier

    def add_point(self, point: Point):
        # Get list of distances to point
        list_tuplesPointDist = self.graph.get_distance_to(point)

        # Get nearest point
        num_nearestPointDistance = list_tuplesPointDist[0][0]

        # Get list of neighbors of point
        filter_tuples = filter(lambda x: x[0] <= num_nearestPointDistance * self.dist_multiplier, list_tuplesPointDist)
        list_tuplesNeighbors = list(filter_tuples)
        map_neighbors = map(lambda x: x[1], list_tuplesNeighbors)
        set_neighbors = set(map_neighbors)

        # Add point and its neighbors to graph
        self.graph.add_point(point, set_neighbors)

        # Get list of points which connections is needed to correct
        list_pointsToCorrect = list(map_neighbors)

        # Correct graph structure
        self.__correct_graph_structure(list_pointsToCorrect)

    def delete_point(self, point: Point):
        # Get list of points which connections is needed to correct
        list_tuplesNeighbors = self.graph.get_neighbours_list(point)
        map_neighbors = map(lambda x: x[1], list_tuplesNeighbors)
        list_pointsToCorrect = list(map_neighbors)

        # Remove point
        self.graph.remove_point(point)

        # Correct graph structure
        self.__correct_graph_structure(self.graph, list_pointsToCorrect)

    def find_nearest_to(self, point: Point):
        pass

    def __correct_graph_structure(self, list_pointsToCorrect: Point):
        """
        This method corrects connections of each point in list_pointsToCorrect
        :param graph: Graph structure you want to correct
        :return: none
        """
        for point in list_pointsToCorrect:
            # Get neighbors of point
            list_tuplesCurrentNeighbors = self.graph.get_neighbours_list(point)

            # Get distance to nearest neighbor
            num_nearestPointDistance = list_tuplesCurrentNeighbors[0][0]

            # Create a list of new neighbors
            filter_tuples = filter(lambda x: x[0] <= num_nearestPointDistance * self.coeff, list_tuplesCurrentNeighbors)
            list_tuplesNewNeighbors = list(filter_tuples)

            # Check if we need don't need to correct current connections
            if len(list_tuplesNewNeighbors) == len(list_tuplesCurrentNeighbors):
                continue

            # Get only points and set them as new neighbors
            map_NewNeighbors = map(lambda x: x[1], list_tuplesNewNeighbors)
            set_NewNeighbors = set(map_NewNeighbors)
            self.graph.set_neighbours(set_NewNeighbors)