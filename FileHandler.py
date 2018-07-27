import logging
import os


logger = logging.getLogger("FileHandler")


class FileHandler:

    # Find last symbol of txt file
    @staticmethod
    def collect_information(file_list):
        logger.info("Collecting content of files started")
        table = []
        for name in file_list:
            if name[-4:] == ".txt":
                if os.path.getsize(name) == 0:
                    continue
                else:
                    with open(name, 'rb') as file:
                        file.seek(1, os.SEEK_END)
                        while file.tell() > 1 and file.seek(-2, os.SEEK_CUR):
                            try:
                                ch = file.read(1).decode()
                                # print(ch, " pos=", file.tell(), " size=", sys.getsizeof(ch))
                                if str(ch).isalpha():
                                    table.append({"path": name, "last_symbol": ch})
                                    logger.info("File read success")
                                    break
                            except UnicodeDecodeError:
                                logger.error("Catch UnicodeDecodeError in ", name)
                                break
                            pass
                        pass
                    pass
                pass
            pass
        return table
