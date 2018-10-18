import os
from statistics import mean

from scipy import misc

from geometry.Point import Point


def get_image_info(image_name):
    image = misc.imread(image_name, False, 'RGB').flatten()
    length = len(image)
    r = [image[i] for i in range(length) if i % 3 == 0]
    r = mean(r)
    g = [image[i] for i in range(length) if i % 3 == 1]
    g = mean(g)
    b = [image[i] for i in range(length) if i % 3 == 2]
    b = mean(b)
    return r, g, b


def get_txtfile_info(filename):
    ch = None
    try:
        with open(filename, 'rb') as file:
            file.seek(1, os.SEEK_END)
            while file.tell() > 1 and file.seek(-3, os.SEEK_CUR) and \
                    (ch.isalpha() or ch.isdigit() or not ch.isalpha()):
                ch = file.read(2).decode()[-1]
        # Исключение если файл невозможно прочитать в следствии неизвестной кодировки
    except UnicodeDecodeError:
        pass
        # Выбрасывается если файл занят другим процессом или просто напросто недоступен для user'a
    except PermissionError:
        pass
        # Выбрасывается в том случае, если в файле меньше одного байта
    except OSError:
        with open(filename, 'rb') as file:
            ch = file.read(1).decode()
    return ch


class ContentGrubber:

    def __init__(self, type_of_file: str):
        if type_of_file == 'image':
            self.get_info = get_image_info
        elif type_of_file == 'txt':
            self.get_info = get_txtfile_info

    def grub(self, filename: str):
        info = self.get_info(filename)
        return info

    pass
