import csv
import logging
from threading import Thread

import pandas as pd

logger = logging.getLogger("TableHandler")


class TableHandler(Thread):

    def __init__(self, target: list):
        self.target = target
        Thread.__init__(self)
        self.start()
        pass

    def run(self):
        self.__create_table()
        pass

    # First create. Fill the table
    def __create_table(self):
        logger.info("First creating table")
        with open("table.csv", "w", newline="") as file:
            columns = ["path", "last_symbol"]
            writer = csv.DictWriter(file, fieldnames=columns)
            writer.writeheader()
            writer.writerows(self.target)
            pass
        pass

    # Delete note
    @staticmethod
    def delete(target):
        logger.info("Deleting ", target)
        try:
            file = pd.read_csv("table.csv", sep=',')
            file = file[file.path != target]
            file.to_csv("table.csv", index=False)
        except UnicodeDecodeError:
            logger.error("Catch UnicodeDecodeError in delete module")
            pass
        pass

    # Create new note
    @staticmethod
    def create(target):
        logger.info("Creating ", target)
        try:
            file = pd.read_csv("table.csv", sep=',')
            file.loc[len(file)] = [target, "SystemMessage: empty file"]
            file.to_csv("table.csv", index=False)
        except UnicodeDecodeError:
            logger.error("Catch UnicodeDecodeError in create module")
            pass
        pass

    # Update content of file in table
    @staticmethod
    def update(update):
        try:
            file = pd.read_csv("table.csv", sep=',')
            for content in update:
                logger.info("Updating ", str(content))
                v = list(content.values())
                file.last_symbol[file.path == str(v[0])] = str(v[1])
            file.to_csv("table.csv", index=False)
        except UnicodeDecodeError:
            logger.error("Catch UnicodeDecodeError in update module")
            pass
        pass

    # Update name(path) of file in table
    @staticmethod
    def rename(before, after):
        logger.info("Renaming fro ", before, " to ", after)
        try:
            file = pd.read_csv("table.csv", sep=',')
            file.path[file.path == before] = after
            file.to_csv("table.csv", index=False)
        except UnicodeDecodeError:
            logger.error("Catch UnicodeDecodeError in rename module")
            pass
        pass
