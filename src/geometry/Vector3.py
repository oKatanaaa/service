from math import tan

class Vector3:
    
    def __init__(self, x: float, y: float, z: float):
        self.x = x
        self.y = y
        self.z = z

    def __str__(self):
        return "({}, {}, {})".format(self.x, self.y, self.z)

    def get_len(self):
        square_sum = self.x*self.x + self.y*self.y + self.z*self.z
        return square_sum**0.5

    def mult_by_const(self, other: float):
        if not isinstance(other, float) and not isinstance(other, int):
            raise Exception("Incorrect argument type: Passed {}".format(type(other)))

        new_vector = Vector3(self.x, self.y, self.z)

        new_vector.x *= other
        new_vector.y *= other
        new_vector.z *= other
        return new_vector
            
    def mult_by_vector(self, other):
        if not isinstance(other, Vector3) and other is not Vector3:
            raise Exception("Incorrect argument type: Passed {}".format(type(other)))

        x1 = self.x
        y1 = self.y
        z1 = self.z

        x2 = other.x
        y2 = other.y
        z2 = other.z

        const1 = y1*z2 - y2*z1
        const2 = x1*z2 - z1*x2
        const3 = x1*y2 - x2*y1

        return Vector3(const1, -const2, const3)

    def mult_scalar(self, other):
        return self.x*other.x + self.y*other.y + self.z*other.z

    def add_vector(self, other):
        return self + other

    def sub_vector(self, other):
        return self - other

    def get_copy(self):
        return Vector3(self.x, self.y, self.z)

    def get_normalized(self):
        return self.get_copy().normalize()

    def normalize(self):
        length = self.get_len()
        self.x /= length
        self.y /= length
        self.z /= length
        return self

    def rotate_in_plane(self, plane, angle: float):
        vec_current = self
        vec_norm = plane.get_norm_vec()

        if(vec_norm.mult_scalar(vec_current) != 0.0):
            raise Exception("Vector must be collinear to the plane!")

        vec_k = vec_current.mult_by_vector(vec_norm)
        vec_f1 = vec_current + tan(angle)*vec_k

        return vec_f1.normalize()*vec_current.get_len()

    @staticmethod
    def vector3_e1():
        return Vector3(1, 0, 0)

    @staticmethod
    def vector3_e2():
        return Vector3(0, 1, 0)

    @staticmethod
    def vector3_e3():
        return Vector3(0, 0, 1)

    # len(Vector3)
    def __len__(self):
        return self.get_len()

    # Vector3 + Vector3
    def __add__(self, other):
        if not isinstance(other, Vector3):
            raise Exception("Incorrect argument type: Passed {}".format(type(other)))

        new_x = self.x + other.x
        new_y = self.y + other.y
        new_z = self.z + other.z
        return Vector3(new_x, new_y, new_z)

    # Vector3 += Vector3
    def __iadd__(self, other):
        self.x += other.x
        self.y += other.y
        self.z += other.z

    # Vector3 - Vector3
    def __sub__(self, other):
        return self.__add__(other.__neg__())

    # Vector3 -= Vector3
    def __isub__(self, other):
        self.__iadd__(other.__neg__())

    # -Vector3
    def __neg__(self):
        return Vector3(-self.x, -self.y, -self.z)

    # Vector3 * Vector3(in math [Vector3, Vector3]) or const * Vector3
    def __mul__(self, other: float):
        if isinstance(other, Vector3):
            return self.mult_by_vector(other)
        elif isinstance(other, float) or isinstance(other, int):
            return self.mult_by_const(other)
        else:
            raise Exception("Incorrect argument type: Passed {}".format(type(other)))

    def __rmul__(self, other: float):
        if isinstance(other, Vector3):
            return self.mult_by_vector(other)
        elif isinstance(other, float) or isinstance(other, int):
            return self.mult_by_const(other)
        else:
            raise Exception("Incorrect argument type: Passed {}".format(type(other)))

    def __eq__(self, other):
        return self.x == other.x and self.y == other.y and self.z == other.z

