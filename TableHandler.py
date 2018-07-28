import csv
import logging
import sys

import pandas as pd

from ClusterBase import ClusterBase
from FileHandler import FileHandler


class TableHandler:
    logger = logging.getLogger("TableHandler")
    # First create. Fill the table

    def __init__(self, name: str = "table.csv"):
        self.name = name
        self.cluster_base = ClusterBase()

    def create_cluster_table(self, target):
        TableHandler.logger.info("First creating cluster table")
        with open(self.name, "w", newline="", encoding='utf-8') as file:
            columns = ["path", "last_symbol", "cluster"]
            writer = csv.DictWriter(file, fieldnames=columns)
            writer.writeheader()
            writer.writerows(target)
            pass
        self.cluster_base.make_cluster()
        TableHandler.logger.info("Table was created")
        pass

    def create_table(self, target):
        TableHandler.logger.info("First creating table")
        self.cluster_base.make_cluster()
        file = pd.DataFrame({'path': [], 'last_symbol': [], 'cluster': []})
        for line in target:
            line['cluster'] = self.cluster_base.go_in_right_cluster(line['last_symbol'])
            file.loc[len(file)] = line
        # with open(self.name, "w", newline="", encoding='utf-8') as file:
        #     columns = ["path", "last_symbol", "cluster"]
        #     writer = csv.DictWriter(file, fieldnames=columns)
        #     writer.writeheader()
        #     writer.writerows(target)
        #     pass
        file.to_csv(self.name, index=False)
        TableHandler.logger.info("Table was created")
        pass


    # Delete note
    def delete(self, target):
        TableHandler.logger.info("Deleting ", target)
        try:
            file = pd.read_csv(self.name, sep=',')
            file = file[file.path != target]
            file.to_csv(self.name, index=False)
        except UnicodeDecodeError:
            TableHandler.logger.error("Catch UnicodeDecodeError in delete module")
            pass
        pass

    # Create new note
    def create(self, target):
        TableHandler.logger.info("Creating ", target)
        try:
            file = pd.read_csv(self.name, sep=',')
            file.loc[len(file)] = [target, "SystemMessage: empty file"]
            file.to_csv(self.name, index=False)
        except UnicodeDecodeError:
            TableHandler.logger.error("Catch UnicodeDecodeError in create module")
            pass
        pass

    # Update content of file in table
    def update(self, full_filename):
        update = FileHandler.collect_information([str(full_filename)])
        try:
            file = pd.read_csv(self.name, sep=',')
            for content in update:
                TableHandler.logger.info("Updating ", str(content))
                content['cluster'] = self.cluster_base.go_in_right_cluster(content['last_symbol'])
                v = list(content.values())
                if file.loc[file['path'] == str(v[2])].empty and sys.getsizeof(full_filename) != 0:
                    file.loc[len(file)] = [str(v[2]), str(v[1]), str(v[0])]
            file.to_csv(self.name, index=False)
        except UnicodeDecodeError:
            TableHandler.logger.error("Catch UnicodeDecodeError in update module")
            pass
        pass

    # Update name(path) of file in table
    def rename(self, before, after):
        TableHandler.logger.info("Renaming fro ", before, " to ", after)
        try:
            file = pd.read_csv(self.name, sep=',')
            file.path[file.path == before] = after
            file.to_csv(self.name, index=False)
        except UnicodeDecodeError:
            TableHandler.logger.error("Catch UnicodeDecodeError in rename module")
            pass
        pass
