import os

import pandas as pd

from cluster_worker.TableRow import TableRow
from geometry.Point import Point


class TableHandler:
    ROOT_PATH = r"Q:/service/output"
    TABLES = list()

    def __init__(self, filename: str, is_teacher: bool):
        self.path = os.path.join(self.ROOT_PATH, filename)
        self.is_teacher = is_teacher
        self.TABLES.append((
            filename,
            is_teacher
        ))
        self.__create_table()
        pass

    def __create_table(self):
        table = pd.DataFrame({'path': [], 'feature': [], 'cluster': []})
        table.columns = ['path', 'feature', 'cluster']
        table.to_csv(self.path, index=False)
        pass

    def rename_file(self, old_name, new_name):
        table = pd.read_csv(self.path, sep=',')
        table.path[table.path == old_name] = new_name
        table.to_csv(self.path, index=False)
        pass

    def delete(self, row: TableRow):
        table = pd.read_csv(self.path, sep=',')
        table = table[table.path != row.get_filename()]
        table.to_csv(self.path, index=False)
        pass

    def update(self, rows: list):
        table = pd.read_csv(self.path, sep=',')
        for row in rows:
            if table[table.path == row.get_filename()].empty:
                table.loc[len(table)] = row.to_dict()
            else:
                # TODO check
                row = row.to_dict()
                table.feature[table['path'] == row['path']] = row['feature']
                table.cluster[table['path'] == row['path']] = row['cluster']
        table.to_csv(self.path, index=False)
        pass

    def get_rows_with_cluster(self, cluster: Point):
        if self.is_teacher:
            raise Exception('Unsupported Operation')
        table = pd.read_csv(self.path, sep=',')
        table = table[table.cluster == cluster.to_str()]
        table = table.to_dict(orient='records')
        return table

    def get_feature(self, filename: str):
        table = pd.read_csv(self.path, sep=',')
        table = table[table.path == filename]
        table = table.to_dict(orient='records')
        if len(table) == 0:
            return None
        return Point(table[0].get('feature').split(' '))
