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
        # This list contains tuples of points and distances to them : (distance, point)
        list_distances = self.graph.get_distance_to(point)

        # If graph is empty we don't need to do any specific operations but adding point
        if len(list_distances) == 0:
            self.graph.add_point(point, set())
            return

        # Get nearest point
        num_nearest_distance = list_distances[0][0]

        # Get list of new neighbors of point
        filter_tuples = filter(lambda x: x[0] <= num_nearest_distance * self.dist_multiplier, list_distances)
        list_neighbors = list(filter_tuples)
        map_neighbors = map(lambda x: x[1], list_neighbors)
        set_neighbors = set(map_neighbors)

        # Add point and its neighbors to graph
        self.graph.add_point(point, set_neighbors)

        # Get list of points which connections is needed to correct
        list_points_to_correct = list(map_neighbors)

        # Correct graph structure
        self.__correct_graph_structure(list_points_to_correct)

    def delete_point(self, point: Point):
        # Get list of points which connections is needed to correct
        list_neighbors = self.graph.get_neighbours_list(point)

        if len(list_neighbors) == 0:
            self.graph.remove_point(point)
            return

        map_neighbors = map(lambda x: x[1], list_neighbors)
        list_points_to_correct = list(map_neighbors)

        # Remove point
        self.graph.remove_point(point)

        # Correct graph structure
        self.__correct_graph_structure(list_points_to_correct)

    def find_nearest_to(self, point: Point):
        set_a = set(self.graph.get_points())

        point_current = set_a.pop()
        num_current_distance = point.distance_to(point_current)

        list_neighbors = self.graph.get_neighbours_list(point_current)
        while len(list_neighbors) != 0:
            # Get shortest distance among neighbors
            list_distances = list(map(lambda x: x[1].distance_to(point), list_neighbors))
            num_new_distance = min(list_distances)

            if num_current_distance < num_new_distance:
                return point_current

            # Update current distance
            num_current_distance = num_new_distance

            # Update point_current
            index_of_new_distance = list_distances.index(num_new_distance)
            point_current = list_neighbors[index_of_new_distance]

            # Update set_a
            for neighbor in list_neighbors:
                set_a.discard(neighbor)

            # Update list_neighbors
            filter_only_a_marked_points = filter(lambda x: x[1] in set_a, self.graph.get_neighbours_list(point_current))
            list_neighbors = list(filter_only_a_marked_points)

        return point_current

    def __correct_graph_structure(self, list_pointsToCorrect: list):
        """
        This method corrects connections of each point in list_pointsToCorrect
        NEED TO MODIFY. DANGER OF LOSS OF CONNECTIONS BETWEEN PARTS OF GRAPH
        :return: none
        """
        for point in list_pointsToCorrect:
            # Get neighbors of point
            list_tuples_current_neighbors = self.graph.get_neighbours_list(point)

            # Get distance to nearest neighbor
            num_nearest_distance = list_tuples_current_neighbors[0][0]

            # Create a list of new neighbors
            filter_tuples = filter(lambda x: x[0] <= num_nearest_distance * self.dist_multiplier,
                                   list_tuples_current_neighbors)
            list_new_neighbors = list(filter_tuples)

            # Check if we need don't need to correct current connections
            if len(list_new_neighbors) == len(list_tuples_current_neighbors):
                continue

            # Get only points and set them as new neighbors
            map_new_neighbors = map(lambda x: x[1], list_new_neighbors)
            set_new_neighbors = set(map_new_neighbors)
            self.graph.set_neighbours(point, set_new_neighbors)
