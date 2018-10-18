from geometry.Point import Point


class TableRow:

    def __init__(self, filename: str, feature, cluster=None):
        self.feature = feature
        self.filename = filename
        self.cluster = cluster
        pass

    def get_filename(self):
        return str(self.filename)

    def get_feature(self):
        result = ""
        for x in self.feature.coordinates:
            result += str(x) + " "
        return result[0:-1]

    def get_cluster(self):
        if self.cluster is not None:
            result = ""
            for x in self.feature.coordinates:
                result += str(x) + " "
            return result[0:-1]
        else:
            return ""

    def to_dict(self):
        return {'path': self.get_filename(),
                'feature': self.get_feature(),
                'cluster': self.get_cluster()}
