import numpy as np


def machine_eps():
    eps = 1.0
    while 1.0 + 0.5 * eps != 1.0:
        eps *= 0.5
    return eps


EPS = machine_eps()


class Point:

    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z

    def to_str(self):
        return str(self.x) + " " + str(self.y) + " " + str(self.y)


class Tetrahedron:

    def __init__(self, points: list):
        self.points = points
        self.neighbors = list()


# noinspection PyMethodMayBeStatic
class TriangulationDelone:
    EPS = machine_eps()

    def __init__(self):
        cube_length = 256
        self.clusters = dict()
        self.tetrahedrons = list()
        self.superstructure = self.build_superstructure(cube_length)

    def build_superstructure(self, cube_length):
        a = Point(cube_length * ((1 - 3 * pow(2, 0.5)) / 2),
                  cube_length * ((1 - pow(3, 0.5)) / 2),
                  cube_length * ((1 - pow(6, 0.5)) / 2))
        b = Point(cube_length * ((1 + 3 * pow(2, 0.5)) / 2),
                  cube_length * ((1 - pow(3, 0.5)) / 2),
                  cube_length * ((1 - pow(6, 0.5)) / 2))
        c = Point(cube_length / 2,
                  cube_length * ((1 - pow(3, 0.5)) / 2),
                  cube_length * ((1 + 2 * pow(6, 0.5)) / 2))
        d = Point(cube_length / 2,
                  cube_length * ((1 + 3 * pow(3, 0.5)) / 2),
                  cube_length / 2)

        return Tetrahedron([a, b, c, d])

    # noinspection PyPep8Naming
    def localize_point(self, plane_point_1: Point, plane_point_2: Point, plane_point_3: Point, plane_point_4: Point,
                       point: Point):
        A = (plane_point_2.y - plane_point_1.y) * (plane_point_3.z - plane_point_1.z) - \
            (plane_point_3.y - plane_point_1.y) * (plane_point_2.z - plane_point_1.z)
        B = (plane_point_3.x - plane_point_1.x) * (plane_point_2.z - plane_point_1.z) - \
            (plane_point_2.x - plane_point_1.x) * (plane_point_3.z - plane_point_1.z)
        C = (plane_point_2.x - plane_point_1.x) * (plane_point_3.y - plane_point_1.y) - \
            (plane_point_3.x - plane_point_1.x) * (plane_point_2.y - plane_point_1.y)
        D = - (A * plane_point_1.x + B * plane_point_1.y + C * plane_point_1.z)
        sign_in_tetrahedron = A * plane_point_4.x + B * plane_point_4.y + C * plane_point_4.z + D
        sign_of_point = A * point.x + B * point.y + C * point.z + D
        if abs(sign_of_point) < EPS:
            return 0  # Если в плоскости или на ребре
        elif sign_in_tetrahedron > 0 and sign_of_point > 0 or sign_in_tetrahedron < 0 and sign_of_point < 0:
            return 1  # С одной стороны
        else:
            return -1  # По разные стороны

    def find_center(self, figure: Tetrahedron):
        big_expr_a = figure.points[3].x ** 2 - figure.points[0].x ** 2 + figure.points[3].y ** 2 - figure.points[
            0].y ** 2 + figure.points[3].z ** 2 - figure.points[0].z ** 2
        big_expr_b = figure.points[3].x ** 2 - figure.points[1].x ** 2 + figure.points[3].y ** 2 - figure.points[
            1].y ** 2 + figure.points[3].z ** 2 - figure.points[1].z ** 2
        big_expr_c = figure.points[3].x ** 2 - figure.points[2].x ** 2 + figure.points[3].y ** 2 - figure.points[
            2].y ** 2 + figure.points[3].z ** 2 - figure.points[2].z ** 2

        big_expr_matrix = np.array([[big_expr_a], [big_expr_b], [big_expr_c]])

        dif_matrix = np.array([[figure.points[3].x - figure.points[0].x, figure.points[3].y - figure.points[0].y,
                                figure.points[3].z - figure.points[0].z],
                               [figure.points[3].x - figure.points[1].x, figure.points[3].y - figure.points[1].y,
                                figure.points[3].z - figure.points[1].z],
                               [figure.points[3].x - figure.points[2].x, figure.points[3].y - figure.points[2].y,
                                figure.points[3].z - figure.points[2].z]])

        dif_matrix_det = 2 * np.linalg.det(dif_matrix)

        x0_up = np.concatenate((big_expr_matrix, dif_matrix[1].reshape((3, 1)), dif_matrix[2].reshape((3, 1))), axis=1)
        x0 = np.linalg.det(x0_up) / dif_matrix_det

        y0_up = np.concatenate(((dif_matrix[0].reshape((3, 1))), big_expr_matrix, dif_matrix[2].reshape((3, 1))),
                               axis=1)
        y0 = np.linalg.det(y0_up) / dif_matrix_det

        z0_up = np.concatenate(((dif_matrix[0].reshape((3, 1))), dif_matrix[1].reshape((3, 1)), big_expr_matrix),
                               axis=1)
        z0 = np.linalg.det(z0_up) / dif_matrix_det

        return Point(x0, y0, z0)

    def find_radius(self, center: Point, point: Point):
        return pow((center.x - point.x) ** 2 + (center.y - point.y) ** 2 + (center.z - point.z) ** 2, 0.5)

    def delone_check(self, point: Point, center: Point, radius: float):
        return (point.x - center.x) ** 2 + (point.x - center.x) ** 2 + (point.x - center.x) ** 2 >= radius ** 2

    def is_on_edge(self, plane: list, point: Point):
        combinations = [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]
        for combination in combinations:
            x = (point.x - plane[combination[0]].x) / (plane[combination[1]].x - plane[combination[0]].x)
            y = (point.y - plane[combination[0]].y) / (plane[combination[1]].y - plane[combination[0]].y)
            z = (point.z - plane[combination[0]].z) / (plane[combination[1]].z - plane[combination[0]].z)
            if x == y == z:
                return combination
        return None

    def find_point(self, point: Point):
        combinations = [(0, 1, 2, 3), (1, 2, 3, 0), (2, 3, 0, 1), (3, 0, 1, 2)]
        target = self.tetrahedrons[0]
        res = -1
        # TODO возможно нужна преинициализация если первые 4 точки
        possibly_plate = None
        for i in range(len(self.tetrahedrons)):
            res = 1
            for comb in combinations:
                location = self.localize_point(target.points[comb[0]], target.points[comb[1]], target.points[comb[2]],
                                               target.points[comb[3]], point)
                if location == -1 or location == 0:
                    possibly_plate = comb
                    res = location
                    break
                res *= location

            # Внутри target - тетраэдра (или на его гранях)
            if res == 1 or res == 0:
                break

            target = self.find_nearby(target, possibly_plate)

        return res, target, possibly_plate

    def find_nearby(self, t: Tetrahedron, plate: tuple):
        plate = t.points[plate[0:3]]
        for ngh in t.neighbors:
            if set(plate).issubset(ngh.points):
                return ngh
        return None

    def delete_tetrahedron(self, t: Tetrahedron):
        points = t.points
        for p in points:
            points_without_p = points
            points_without_p.remove(p)
            for pp in points_without_p:
                self.clusters[p.to_str()].remove(pp)
        self.tetrahedrons.remove(t)
        for ngh in t.neighbors:
            ngh.remove(t)
        return t.neighbors

    # noinspection PyUnboundLocalVariable
    def update_neighbours(self, ngh_of_deleted: list, new_figure_list: list):
        combinations = [(0, 1, 2, 3), (1, 2, 3, 0), (2, 3, 0, 1), (3, 0, 1, 2)]
        for figure in new_figure_list:
            copy = new_figure_list
            copy.remove(figure)
            figure.neighbors = copy
            for ngh in ngh_of_deleted:
                for comb in combinations:
                    # TODO чекнуть верно работает или нет
                    figure_plate_list = [figure.points[i] for i in comb]
                for comb in combinations:
                    # TODO чекнуть верно работает или нет
                    ngh_plate_list = [ngh.points[i] for i in comb]
                if len(set(figure_plate_list) & set(ngh_plate_list)) != 0:
                    figure.neighbors.append(ngh)
                    ngh.neighbors.append(figure)

    def add_in_tetrahedron(self, tetrahedron: Tetrahedron, point: Point):
        combinations = [(0, 1, 2, 3), (1, 2, 3, 0), (2, 3, 0, 1), (3, 0, 1, 2)]
        new_figure_list = []
        for comb in combinations:
            new_figure_list.append(Tetrahedron([point, tetrahedron.points[comb[0]], tetrahedron.points[comb[1]],
                                                tetrahedron.points[comb[2]]]))
        return new_figure_list

    def add_in_plate(self, tetrahedron: Tetrahedron, plate: list, point: Point):
        other_points = list(set(tetrahedron.points) - set(plate))
        new_figure_list = [Tetrahedron([point, plate[0], plate[1], other_points[0]]),
                           Tetrahedron([point, plate[0], plate[2], other_points[0]]),
                           Tetrahedron([point, plate[1], plate[2], other_points[0]])]
        return new_figure_list

    def add_in_edge(self, tetrahedron: Tetrahedron, edge: list, point: Point):
        other_point = list(set(tetrahedron.points) - set(edge))
        new_figure_list = [Tetrahedron([point, edge[0], other_point[0], other_point[1]]),
                           Tetrahedron([point, edge[1], other_point[0], other_point[1]])]
        return new_figure_list

    def add_outside(self, plate: list, point: Point):
        return [Tetrahedron([plate[0], plate[1], plate[2], point])]

    def check_local_rebuilding(self, new_figure_list: list):
        for figure in new_figure_list:
            center = self.find_center(figure)
            radius = self.find_radius(center, figure.points[0])
            check_list = new_figure_list
            check_list.remove(figure)
            for check in check_list:
                for check_point in check.points:
                    if not self.delone_check(check_point, center, radius):
                        type_of_rebuild = self.identify_type(figure, check)
                        if type_of_rebuild == 1:
                            new_figure_list.remove(figure)
                            new_figure_list.remove(check)
                            new_figure_list += self.local_rebuild_first(figure, check)
                            self.check_local_rebuilding(new_figure_list)
                        else:
                            new_figure_list.remove(figure)
                            new_figure_list.remove(check)
                            update = list(self.local_rebuild_second(figure, check))
                            if update[3] in new_figure_list:
                                new_figure_list.remove(update[3])
                            update.pop()
                            new_figure_list += update
                            self.check_local_rebuilding(new_figure_list)
        return new_figure_list

    # noinspection PyPep8Naming
    def identify_type(self, first: Tetrahedron, second: Tetrahedron):
        plate = set(first.points) & set(second.points)
        out_first = list(set(first.points) - plate)[0]
        out_second = list(set(second.points) - plate)[0]
        plate = list(plate)
        triangle = [plate[0], out_first, out_second]
        edge = [plate[1], plate[2]]
        A = (triangle[1].y - triangle[0].y) * (triangle[2].z - triangle[0].z) - \
            (triangle[2].y - triangle[0].y) * (triangle[1].z - triangle[0].z)
        B = (triangle[2].x - triangle[0].x) * (triangle[1].z - triangle[0].z) - \
            (triangle[1].x - triangle[0].x) * (triangle[2].z - triangle[0].z)
        C = (triangle[1].x - triangle[0].x) * (triangle[2].y - triangle[0].y) - \
            (triangle[2].x - triangle[0].x) * (triangle[1].y - triangle[0].y)
        D = - (A * triangle[0].x + B * triangle[0].y + C * triangle[0].z)

        xs = edge[0].x - edge[1].x
        ys = edge[0].y - edge[1].y
        zs = edge[0].z - edge[1].z

        t = - (A * edge[1].x + B * edge[1].y + C * edge[1].z + D) / (A * xs + B * ys + C * zs)

        xp = edge[1].x + xs * t
        yp = edge[1].y + ys * t
        zp = edge[1].z + zs * t

        d_ab = self.distance(triangle[0].x, triangle[1].x, triangle[0].y, triangle[1].y, triangle[0].z, triangle[1].z)
        d_bc = self.distance(triangle[2].x, triangle[1].x, triangle[2].y, triangle[1].y, triangle[2].z, triangle[1].z)
        d_ac = self.distance(triangle[0].x, triangle[2].x, triangle[0].y, triangle[2].y, triangle[0].z, triangle[2].z)
        d_ap = self.distance(triangle[0].x, xp, triangle[0].y, yp, triangle[0].z, zp)
        d_bp = self.distance(triangle[1].x, xp, triangle[1].y, yp, triangle[1].z, zp)
        d_cp = self.distance(triangle[2].x, xp, triangle[2].y, yp, triangle[2].z, zp)

        p0 = (d_ab + d_ac + d_bc) / 2.
        p1 = (d_ab + d_ap + d_bp) / 2.
        p2 = (d_ap + d_cp + d_bc) / 2.
        p3 = (d_ap + d_ac + d_cp) / 2.

        s0 = pow(p0 * (p0 - d_ab) * (p0 - d_ac) * (p0 - d_bc), 0.5)
        s1 = pow(p1 * (p1 - d_ab) * (p1 - d_ap) * (p1 - d_bp), 0.5)
        s2 = pow(p2 * (p2 - d_ap) * (p2 - d_cp) * (p2 - d_bc), 0.5)
        s3 = pow(p3 * (p3 - d_ap) * (p3 - d_ac) * (p3 - d_cp), 0.5)

        if s0 == s1 + s2 + s3:
            return 1
        else:
            return 2

    def local_rebuild_first(self, first: Tetrahedron, second: Tetrahedron):
        plate = set(first.points) & set(second.points)
        out_first = list(set(first.points) - plate)[0]
        out_second = list(set(second.points) - plate)[0]
        plate = list(plate)
        new_first = Tetrahedron([plate[0], plate[1], out_first, out_second])
        new_second = Tetrahedron([plate[1], plate[2], out_first, out_second])
        new_third = Tetrahedron([plate[0], plate[2], out_first, out_second])
        return new_first, new_second, new_third

    def local_rebuild_second(self, first: Tetrahedron, second: Tetrahedron):
        plate = set(first.points) & set(second.points)
        out_first = list(set(first.points) - plate)[0]
        out_second = list(set(second.points) - plate)[0]
        plate = list(plate)
        new_first = Tetrahedron([plate[0], plate[1], out_first, out_second])
        new_second = Tetrahedron([plate[0], plate[1], plate[2], out_second])
        new_third = Tetrahedron([plate[0], plate[2], out_first, out_second])
        delete_t = Tetrahedron([plate[1], plate[2], out_first, out_second])
        return new_first, new_second, new_third, delete_t

    def add_point(self, coordinate: str):
        if coordinate not in self.clusters:
            if len(self.clusters.keys()) < 4:
                keys = list(self.clusters.keys())
                self.clusters[coordinate] = set()
                for key in keys:
                    self.clusters[coordinate].add(key)
                    self.clusters[key].add(coordinate)
                if len(keys) == 3:
                    points_str = self.clusters.get(coordinate)
                    points_str.append(coordinate)
                    points = []
                    for p in points_str:
                        p = p.split(" ")
                        points.append(Point(p[0], p[1], p[2]))
                    self.tetrahedrons.append(Tetrahedron(points))

            coordinate = coordinate.split(" ")
            point = Point(coordinate[0], coordinate[1], coordinate[2])
            res, target, possibly_plate = self.find_point(point)
            new_figure_list = None
            if res > 0:
                new_figure_list = self.add_in_tetrahedron(target, point)
            elif res < 0:
                new_figure_list = self.add_outside(target.points[possibly_plate[0:3]], point)
            else:  # res == 0
                edge = self.is_on_edge(target.points[possibly_plate[0:3]], point)
                if edge is not None:
                    new_figure_list = self.add_in_edge(target, target.points[edge[0:2]], point)

            new_figure_list = self.check_local_rebuilding(new_figure_list)
            ngh_of_deleted = self.delete_tetrahedron(target)
            self.update_neighbours(ngh_of_deleted, new_figure_list)
            self.tetrahedrons += new_figure_list
            self.update_cluster(new_figure_list)

    def update_cluster(self, new_figure_list: list):
        for figure in new_figure_list:
            points = figure.points
            for p in points:
                points_without_p = points
                points_without_p.remove(p)
                for pp in points_without_p:
                    self.clusters[p.to_str()].add(pp)

    def delete_point(self, point: Point):
        pass

    def distance(self, x1, x2, y1, y2, z1, z2):
        return pow((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2) + (z1 - z2) * (z1 - z2), 0.5)
