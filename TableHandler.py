from threading import Thread
import csv


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


# if __name__ == "__main__":
#     target = [{"path": '/home/banayaki/PycharmProjects/service/rrr.txt', "last_line": '888dddфыав\n'},
#               {"path": '/home/banayaki/PycharmProjects/service/rrr.txt', "last_line": '888dddфыав\n'}]
#     handler = TableHandler(target)

