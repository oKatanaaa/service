from geometry.Point import Point


class TableRow:

    def __init__(self, filename: str, feature: Point, cluster=None):
        self.feature = feature
        self.filename = filename
        self.cluster = cluster
        pass

