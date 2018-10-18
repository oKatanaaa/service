from cluster_worker.TableHandler import TableHandler
from cluster_worker.TableRow import TableRow
from geometry.Point import Point


class ClusterHandler:

    def __init__(self, algorithm):
        self.file_table_handler = TableHandler('cluster_table.csv', True)
        self.teacher_table_handler = TableHandler('file_table.csv', False)
        self.algorithm = algorithm

    def create_cluster(self, row: TableRow):
        if not self.algorithm.is_in_graph(row.feature):
            self.algorithm.add_point(row.feature)
            # Need to cast
            self.teacher_table_handler.update([row])
            self.soft_update(row)
        pass

    def delete_cluster(self, cluster_row: TableRow):
        point = cluster_row.feature
        neighbours = self.algorithm.graph.get_neighbours(point)
        rows = self.file_table_handler.get_rows_with_cluster(point)
        for row in rows:
            row.cluster = self.__choose_nearest(row.feature, neighbours)
        self.file_table_handler.update(rows)
        self.teacher_table_handler.delete(cluster_row)
        pass

    def hard_update(self, rows: list):
        for row in rows:
            point = row.feature
            row.cluster = self.algorithm.find_nearest_to(point)
        # Need to cast
        self.file_table_handler.update(rows)
        pass

    def soft_update(self, cluster_row: TableRow):
        new_cluster = cluster_row.feature
        neighbours = self.algorithm.graph.get_neighbours(new_cluster)
        for neighbour in neighbours:
            rows = self.file_table_handler.get_rows_with_cluster(neighbour)
            for row in rows:
                current_cluster = Point(row.cluster)
                feature = Point(row.feature)
                if self.__new_nearest(feature, current_cluster, new_cluster):
                    row.cluster = new_cluster
            self.file_table_handler.update(rows)
        pass

    # noinspection PyMethodMayBeStatic
    def __new_nearest(self, feature, current, new):
        return feature.distance_to(current) > feature.distance_to(new)

    # noinspection PyMethodMayBeStatic
    def __choose_nearest(self, feature, neighbours):
        if len(neighbours) == 0:
            return None
        dif = float('inf')
        nearest = None
        for ngh in neighbours:
            dist = feature.distance_to(ngh)
            if dist < dif:
                dif = dist
                nearest = ngh
        return nearest
