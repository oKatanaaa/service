from geometry import Point


def distance(first: Point, second: Point):

    if len(first.coordinates) != len(second.coordinates):
        raise Exception()
    sum_of_squares = 0

    for p1, p2 in zip(first, second):
        sum_of_squares += (p1 - p2) ** 2

    return sum_of_squares ** 0.5

