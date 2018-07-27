import csv
import logging

import pandas as pd

from FileHandler import FileHandler

logger = logging.getLogger("TableHandler")


class TableHandler:

    # First create. Fill the table
    @staticmethod
    def create_table(target):
        logger.info("First creating table")
        with open("table.csv", "w", newline="") as file:
            columns = ["path", "last_symbol"]
            writer = csv.DictWriter(file, fieldnames=columns)
            writer.writeheader()
            writer.writerows(target)
            pass
        logger.info("Table was created")
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
    def update(full_filename):
        update = FileHandler.collect_information([str(full_filename)])
        try:
            file = pd.read_csv("table.csv", sep=',')
            for content in update:
                logger.info("Updating ", str(content))
                v = list(content.values())
                if not (str(v[0]) in file.path):
                    file.loc[len(file)] = [str(v[0]), ""]
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
