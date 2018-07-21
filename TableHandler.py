import csv
from threading import Thread

import pandas as pd


class TableHandler(Thread):

    def __init__(self, target: list):
        self.target = target
        Thread.__init__(self)
        self.start()

    def run(self):
        self.__create_table()
        pass

    def __create_table(self):
        with open("table.csv", "w", newline="") as file:
            columns = ["path", "last_line"]
            writer = csv.DictWriter(file, fieldnames=columns)
            writer.writeheader()
            writer.writerows(self.target)

    @staticmethod
    def delete(target):
        # Удаляет но появляются какие то новые стобцы
        try:
            file = pd.read_csv("table.csv", sep=',')
            file = file[file.path != str(target)]
            file.to_csv("table.csv", index=False)
        except UnicodeDecodeError:
            # logger.error()
            pass
        pass

    @staticmethod
    def create(target):
        try:
            file = pd.read_csv("table.csv", sep=',')
            file.loc[len(file)] = [target, "SystemMessage: empty file"]
            file.to_csv("table.csv", index=False)
        except UnicodeDecodeError:
            # logger.error()
            pass
        pass

    @staticmethod
    def update(update):
        try:
            file = pd.read_csv("table.csv", sep=',')
            for content in update:
                # Мне нужны только значения
                v = list(content.values())
                file.last_line[file.path == str(v[0])] = str(v[1])
            file.to_csv("table.csv", index=False)
        except UnicodeDecodeError:
            # logger.error
            pass
        pass

    @staticmethod
    def rename(before, after):
        try:
            file = pd.read_csv("table.csv", sep=',')
            file.loc[before, "path"] = after
            file.to_csv("table.csv", index=False)
        except UnicodeDecodeError:
            # logger.error
            pass
        pass

# if __name__ == "__main__":
#     target = [{"path": '/home/banayaki/PycharmProjects/service/rrr.txt', "last_line": '888dddфыав\n'},
#               {"path": '/home/banayaki/PycharmProjects/service/rrr.txt', "last_line": '888dddфыав\n'}]
#     handler = TableHandler(target)
