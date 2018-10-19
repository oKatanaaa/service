from multiprocessing import Queue
from threading import Thread

from cluster_worker.ClusterHandler import ClusterHandler
from cluster_worker.TableRow import TableRow
from cluster_worker.algorithms.nearest_neighbours.NNA import NNA
from file_worker.FileHandler import FileHandler


class DistributionSystem(Thread):

    def __init__(self, teach_path: str, analyze_path: str, alg_type: str, service=None):
        super().__init__()
        self.setName("DistributionSystem " + str(self.ident))
        self.jobs = Queue()
        self.teach_file_handler = FileHandler(teach_path, True, self.jobs)
        self.target_file_handler = FileHandler(analyze_path, False, self.jobs)
        self.service = service

        alg = None
        if alg_type == "delaunay_projection":
            alg = None
        elif alg_type == "neighbour_relations":
            alg = None
        elif alg_type == "nearest_neighbours":
            alg = NNA()

        self.cluster_handler = ClusterHandler(alg)

    def run(self):
        if self.service is None:
            self.start_silent()
        else:
            self.start_verbose()

    def start_silent(self):
        self.teach_file_handler.start()
        self.target_file_handler.start()

        while True:
            job = self.jobs.get()

            action = job.get('action')
            teacher = job.get('teacher')
            row = job.get('row')

            if action == 2:
                if not teacher:
                    self.cluster_handler.just_delete(row)
                else:
                    self.cluster_handler.delete_cluster(row)

            elif action == 3:
                if not teacher:

                    self.cluster_handler.hard_update(row)
                else:
                    self.cluster_handler.update_cluster(row)

            elif action == 5:
                old_name = job.get('old_name')
                new_name = job.get('new_name')
                if teacher:
                    self.cluster_handler.teacher_table_handler.rename_file(old_name, new_name)
                else:
                    self.cluster_handler.file_table_handler.rename_file(old_name, new_name)

    def start_verbose(self):
        self.teach_file_handler.start()
        self.target_file_handler.start()

        check_set = [10, 20, 30]
        ident_set = [30]
        delete_set = [10]
        add_check = 0
        ident_check = 0
        delete_check = 0

        temp = TableRow(None, None)
        while True:
            job = self.jobs.get()

            action = job.get('action')
            teacher = job.get('teacher')
            row = job.get('row')
            if row.get_filename() == temp.get_filename() and row.get_feature() == temp.get_feature():
                continue
            temp = row

            if action == 2:
                if not teacher:
                    self.cluster_handler.just_delete(row)
                else:
                    delete_check += 1
                    self.cluster_handler.delete_cluster(row)
                    if delete_check in delete_set:
                        delete_set.remove(delete_check)
                        self.service.put("Signal")

            elif action == 3:
                if not teacher:
                    ident_check += 1
                    self.cluster_handler.hard_update(row)
                    if ident_check in ident_set:
                        ident_set.remove(ident_check)
                        self.service.put("Signal")
                else:
                    add_check += 1
                    self.cluster_handler.update_cluster(row)
                    if add_check in check_set:
                        check_set.remove(add_check)
                        self.service.put("Signal")

            elif action == 5:
                old_name = job.get('old_name')
                new_name = job.get('new_name')
                if teacher:
                    self.cluster_handler.teacher_table_handler.rename_file(old_name, new_name)
                else:
                    self.cluster_handler.file_table_handler.rename_file(old_name, new_name)
