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
    @staticmethod
    def collect_information(file_list):
        table = []
        for name in file_list:
            if name[-4:] == ".txt":
                if os.path.getsize(name) == 0:
                    table.append({"path": str(name), "last_line": "SystemMessage: empty file"})
                else:
                    with open(name, 'rb') as file:
                        file.seek(os.SEEK_END)
                        while not str(file.read(1).decode()).isspace():  # Может быть a < Z НУЖНО
                            file.seek(-1, os.SEEK_CUR)
                        try:
                            table.append({"path": str(name), "last_line": str(file.readline().decode())})
                        except UnicodeDecodeError:
                            table.append({"path": str(name), "last_line": "SystemMessage: can't decode bytes"})
        return table


if __name__ == "__main__":
    # file_list = ["/home/banayaki/PycharmProjects/service/rrr.txt"]
    # handler = FileHandler(file_list)
    # print(handler.table)
    pass
