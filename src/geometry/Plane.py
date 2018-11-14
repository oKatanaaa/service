from Vector3 import Vector3 as Vec3
from Point import Point
import add_math

class Plane:
    def __init__(self, a, b, c, d):
        self.a = a
        self.b = b
        self.c = c
        self.d = d

    @staticmethod
    def plane_by_norm_vec_and_point(vec_norm, point):
        a = vec_norm.x
        b = vec_norm.y
        c = vec_norm.z
        d = a*point.get_coordinate(0) + b*point.get_coordinate(1) + c*point.get_coordinate(2)
        return Plane(a, b, c, -d)


    @staticmethod
    def plane_by_vecs_and_point():
        pass

    @staticmethod
    def plane_X():
        return Plane(1, 0, 0, 0)

    @staticmethod
    def plane_Y():
        return Plane(0, 1, 0, 0)

    @staticmethod
    def plane_Z():
        return Plane(0, 0, 1, 0)

    def get_norm_vec(self):
        return Vec3(self.a, self.b, self.c).normalize()

    def get_point_relation(self, point: Point):
        """
        This function checks the point position relative to the plane.
        Return values:
            1 - if point is not on the plane, but in space half where the norm vector watches
            -1 - if point is not on the plane, but in space half where the negative norm vector watches
            0 - if point is on the plane
        :param point: point we gonna check
        :return: int
        """
        x = point.get_coordinate(0)
        y = point.get_coordinate(1)
        z = point.get_coordinate(2)
        result = self.a * x + self.b * y + self.c * z + self.d

        return add_math.sign(result)


