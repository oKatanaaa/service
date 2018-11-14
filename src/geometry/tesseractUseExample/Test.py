from geometry.Point import Point
from geometry.Tesseract import Tesseract

tesseract = Tesseract(Point([0,0,0]))
current_neighbors = [Point([0,1,0]), Point([0,2,0])]
other_points = [Point([1,0,0]), Point([0, -1, 0]), Point([-1, 0, 0]), Point([0, 0, 1]), Point([0, 0, -1]), Point([0, 0, -2])]

for point in tesseract.get_new_neighbors(current_neighbors, other_points):
    print(point)