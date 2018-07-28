import logging
import os


class FileHandler:
    logger = logging.getLogger("FileHandler")

    # Find last symbol of txt file
    @staticmethod
    def collect_information(file_list):
        FileHandler.logger.info("Collecting content of files started")
        table = []
        try:
            for name in file_list:
                if name[-4:] == ".txt":
                    if os.path.getsize(name) == 0:
                        continue
                    else:
                        with open(name, 'rb') as file:
                            try:
                                file.seek(1, os.SEEK_END)
                                while file.tell() > 1 and file.seek(-3, os.SEEK_CUR):
                                    ch = file.read(2).decode()
                                    ch = ch[-1]
                                    # print(ch, " pos=", file.tell(), " size=", sys.getsizeof(ch))
                                    if ch.isalpha():
                                        table.append({"path": name, "last_symbol": ch})
                                        FileHandler.logger.info("File read success")
                                        break
                            except UnicodeDecodeError:
                                FileHandler.logger.error("Catch UnicodeDecodeError in ", name)
                            except OSError:
                                with open(name, 'rb') as file:
                                    ch = file.read(1).decode()
                                    ch = ch[-1]
                                    # print(ch, " pos=", file.tell(), " size=", sys.getsizeof(ch))
                                    if ch.isalpha():
                                        table.append({"path": name, "last_symbol": ch})
                                        FileHandler.logger.info("File read success")
                                pass
                            pass
                        pass
                    pass
                pass
            pass
        except PermissionError:
            FileHandler.logger.error("Catch permission error, someone using the file")
        return table
