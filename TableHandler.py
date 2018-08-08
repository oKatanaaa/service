import logging
import sys

import pandas as pd

from ClusterHandler import ClusterHandler
from FileHandler import FileHandler


class TableHandler:
    # First create. Fill the table

    def __init__(self, is_learn: bool):
        self.is_learn = is_learn
        self.logger = self.__get_logger("TableHandler")
        if is_learn:
            self.name = "teach_table.csv"
        else:
            self.name = "table.csv"
        self.cluster_handler = ClusterHandler()

    def create_table(self, target):
        self.logger.info("First creating table")
        file = pd.DataFrame({'path': [], 'last_symbol': [], 'cluster': []})
        file.columns = ['path', 'last_symbol', 'cluster']
        if not self.is_learn:
            for line in target:
                line['cluster'] = self.cluster_handler.go_in_right_cluster(line['last_symbol'])
                file.loc[len(file)] = line
        else:
            for line in target:
                self.cluster_handler.add_new_cluster(line['last_symbol'])
                line['cluster'] = ''
                file.loc[len(file)] = line
        file.to_csv(self.name, index=False)
        self.logger.info("Table was created")
        pass

    # Delete note
    def delete(self, target):
        self.logger.info("Deleting ", target)
        try:
            file = pd.read_csv(self.name, sep=',')
            file = file[file.path != target]
            file.to_csv(self.name, index=False)
        except UnicodeDecodeError:
            self.logger.error("Catch UnicodeDecodeError in delete module")
            pass
        pass

    # Create new note
    def create(self, target):
        self.logger.info("Creating ", target)
        try:
            file = pd.read_csv(self.name, sep=',')
            if file[file.path == target] is None:
                file.loc[len(file)] = [target, "SystemMessage: empty file", ""]
            file.to_csv(self.name, index=False)
        except UnicodeDecodeError:
            self.logger.error("Catch UnicodeDecodeError in create module")
            pass
        pass

    # Update content of file in table
    def update(self, full_filename):
        update = FileHandler.collect_information([str(full_filename)])
        try:
            file = pd.read_csv(self.name, sep=',')
            for content in update:
                self.logger.info("Updating ", str(content))

                if not self.is_learn:
                    content['cluster'] = self.cluster_handler.go_in_right_cluster(content['last_symbol'])
                    v = list(content.values())
                    if file.loc[file['path'] == str(v[0])].empty and sys.getsizeof(full_filename) != 0:
                        file.loc[len(file)] = [str(v[0]), str(v[1]), str(v[2])]
                    else:
                        file.loc[file['path'] == v[0]] = [str(v[0]), str(v[1]), str(v[2])]
                else:
                    # Странно выглядит что то, я запутался
                    v = list(content.values())
                    if file.loc[file['path'] == str(v[0])].empty and sys.getsizeof(full_filename) != 0:
                        file.loc[len(file)] = [str(v[0]), str(v[1])]
                    else:
                        file.loc[file['path'] == str(v[0])] = [str(v[0]), str(v[1])]
                    self.cluster_handler.add_new_cluster(v[1])

            file.to_csv(self.name, index=False)
        except UnicodeDecodeError:
            self.logger.error("Catch UnicodeDecodeError in update module")
            pass
        pass

    # Update name(path) of file in table
    def rename(self, before, after):
        self.logger.info("Renaming fro ", before, " to ", after)
        try:
            file = pd.read_csv(self.name, sep=',')
            file.path[file.path == before] = after
            file.to_csv(self.name, index=False)
        except UnicodeDecodeError:
            self.logger.error("Catch UnicodeDecodeError in rename module")
            pass
        pass

    def __get_logger(self, logger_name: str):
        logger = logging.getLogger("Watcher " + logger_name)
        logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler(".\logs\my_watch.log")
        fh.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'))
        logger.addHandler(fh)
        return logger
