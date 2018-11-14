from Plane import Plane
from Vector3 import Vector3 as Vec3
from Point import Point
import add_math


class Tesseract(object):
    def __init__(self, point: Point):
        self.point = point
        self.conus_x = Tesseract.SquareConus.square_conus_x(point)
        self.conus_y = Tesseract.SquareConus.square_conus_y(point)
        self.conus_z = Tesseract.SquareConus.square_conus_z(point)

    def get_new_neighbors(self, current_neighbors, other_points):
        new_neighbors = []
        current_neighbors_in_x_neg = list(
            filter(lambda point: self.conus_x.is_in_negative_part(point), current_neighbors))
        current_neighbors_in_x_pos = list(
            filter(lambda point: self.conus_x.is_in_positive_part(point), current_neighbors))
        current_neighbors_in_y_neg = list(
            filter(lambda point: self.conus_y.is_in_negative_part(point), current_neighbors))
        current_neighbors_in_y_pos = list(
            filter(lambda point: self.conus_y.is_in_positive_part(point), current_neighbors))
        current_neighbors_in_z_neg = list(
            filter(lambda point: self.conus_z.is_in_negative_part(point), current_neighbors))
        current_neighbors_in_z_pos = list(
            filter(lambda point: self.conus_z.is_in_positive_part(point), current_neighbors))

        points_in_sectors = [current_neighbors_in_x_neg, current_neighbors_in_x_pos, current_neighbors_in_y_neg,
                             current_neighbors_in_y_pos, current_neighbors_in_z_neg, current_neighbors_in_z_pos]

        # Sectors are illustrated as a methods here.
        sectors = [self.conus_x.is_in_negative_part, self.conus_x.is_in_positive_part, self.conus_y.is_in_negative_part,
                   self.conus_y.is_in_positive_part, self.conus_z.is_in_negative_part, self.conus_z.is_in_positive_part]

        for i in range(len(sectors)):
            if len(points_in_sectors[i]) == 0:
                other_points_in_sector = sorted(list(filter(lambda point: sectors[i](point), other_points)),
                                                key=lambda point: point.distance_to(self.point))
                if len(other_points_in_sector) != 0:
                    new_neighbors.append(other_points_in_sector[0])

        return new_neighbors

    class SquareConus(object):
        def __init__(self, center: Point, vec_norm1: Vec3, vec_norm2: Vec3, vec_norm3: Vec3, vec_norm4: Vec3):
            self.plane1 = Plane.plane_by_norm_vec_and_point(vec_norm1, center)
            self.plane2 = Plane.plane_by_norm_vec_and_point(vec_norm2, center)
            self.plane3 = Plane.plane_by_norm_vec_and_point(vec_norm3, center)
            self.plane4 = Plane.plane_by_norm_vec_and_point(vec_norm4, center)

        def is_in_positive_part(self, point: Point):
            rel1 = self.plane1.get_point_relation(point)
            rel2 = self.plane2.get_point_relation(point)
            rel3 = self.plane3.get_point_relation(point)
            rel4 = self.plane4.get_point_relation(point)
            return (rel1 == rel2 == rel3 == rel4 == 1 or  # Point is out of plains
                    rel1 == 0 and rel2 == rel3 == rel4 == 1 or  # Point is on plane1
                    rel2 == 0 and rel1 == rel3 == rel4 == 1 or  # Point is on plane2
                    rel3 == 0 and rel1 == rel2 == rel4 == 1 or  # Point is on plane3
                    rel4 == 0 and rel1 == rel2 == rel3 == 1 or  # Point is on plane4
                    rel1 == rel3 == 0 and rel2 == rel4 == 1 or  # Point is on plane1 and plane3
                    rel1 == rel4 == 0 and rel2 == rel3 == 1 or  # Point is on plane1 and plane4
                    rel2 == rel3 == 0 and rel1 == rel4 == 1 or  # Point is on plane2 and plane3
                    rel2 == rel4 == 0 and rel1 == rel3 == 1)  # Point is on plane2 and plane4

        def is_in_negative_part(self, point: Point):
            rel1 = self.plane1.get_point_relation(point)
            rel2 = self.plane2.get_point_relation(point)
            rel3 = self.plane3.get_point_relation(point)
            rel4 = self.plane4.get_point_relation(point)
            return (rel1 == rel2 == rel3 == rel4 == -1 or  # Point is out of plains
                    rel1 == 0 and rel2 == rel3 == rel4 == -1 or  # Point is on plane1
                    rel2 == 0 and rel1 == rel3 == rel4 == -1 or  # Point is on plane2
                    rel3 == 0 and rel1 == rel2 == rel4 == -1 or  # Point is on plane3
                    rel4 == 0 and rel1 == rel2 == rel3 == -1 or  # Point is on plane4
                    rel1 == rel3 == 0 and rel2 == rel4 == -1 or  # Point is on plane1 and plane3
                    rel1 == rel4 == 0 and rel2 == rel3 == -1 or  # Point is on plane1 and plane4
                    rel2 == rel3 == 0 and rel1 == rel4 == -1 or  # Point is on plane2 and plane3
                    rel2 == rel4 == 0 and rel1 == rel3 == -1)  # Point is on plane2 and plane4

        @staticmethod
        def square_conus_x(point: Point):
            vec_norm1 = Vec3.vector3_e1() + Vec3.vector3_e2()
            vec_norm2 = Vec3.vector3_e1() - Vec3.vector3_e2()
            vec_norm3 = Vec3.vector3_e1() + Vec3.vector3_e3()
            vec_norm4 = Vec3.vector3_e1() - Vec3.vector3_e3()
            return Tesseract.SquareConus(point, vec_norm2, vec_norm2, vec_norm3, vec_norm4)

        @staticmethod
        def square_conus_y(point: Point):
            vec_norm1 = Vec3.vector3_e2() + Vec3.vector3_e1()
            vec_norm2 = Vec3.vector3_e2() - Vec3.vector3_e1()
            vec_norm3 = Vec3.vector3_e2() + Vec3.vector3_e3()
            vec_norm4 = Vec3.vector3_e2() - Vec3.vector3_e3()
            return Tesseract.SquareConus(point, vec_norm2, vec_norm2, vec_norm3, vec_norm4)

        @staticmethod
        def square_conus_z(point: Point):
            vec_norm1 = Vec3.vector3_e3() + Vec3.vector3_e2()
            vec_norm2 = Vec3.vector3_e3() - Vec3.vector3_e2()
            vec_norm3 = Vec3.vector3_e3() + Vec3.vector3_e1()
            vec_norm4 = Vec3.vector3_e3() - Vec3.vector3_e1()
            return Tesseract.SquareConus(point, vec_norm2, vec_norm2, vec_norm3, vec_norm4)
