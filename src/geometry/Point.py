import hashlib


class Point:

    def __init__(self, coordinates):
        self.coordinates = list()
        for number in coordinates:
            self.coordinates.append(int(number))

    def get_coordinate(self, index: int):
        return self.coordinates[index]

    def distance_to(self, point):
        if len(self.coordinates) != len(point.coordinates):
            raise Exception()
        sum_of_squares = 0

        for p1, p2 in zip(self.coordinates, point.coordinates):
            sum_of_squares += (p1 - p2) ** 2

        return sum_of_squares ** 0.5

    def to_str(self):
        result = ""
        for x in self.coordinates:
            result += str(x) + " "
        return result[0:-1]

    def __hash__(self):
        return self.to_str().__hash__()

    def __eq__(self, compare_to):
        return compare_to.__hash__() == self.__hash__()