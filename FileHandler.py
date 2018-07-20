import os
from threading import Thread

from TableHandler import TableHandler


class FileHandler(Thread):

    def __init__(self, file_list):
        Thread.__init__(self)
        self.file_list = file_list
        self.start()
        self.join()

    def run(self):
        TableHandler(self.collect_information(self.file_list))
        self.file_list = None
        pass

    # Переходит в конец стрима и ищет ближайший перевод строки и выводит строку после последнего перевода
    def collect_information(self, file_list):
        table = []
        for name in file_list:
            if name[-4:] == ".txt":
                with open(name, 'rb') as file:
                    file.seek(-2, os.SEEK_END)
                    while file.read(1) != b'\n':
                        file.seek(-2, os.SEEK_CUR)
                    table.append({"path": str(name), "last_line": str(file.readline().decode())})
        return table


if __name__ == "__main__":
    # file_list = ["/home/banayaki/PycharmProjects/service/rrr.txt"]
    # handler = FileHandler(file_list)
    # print(handler.table)
