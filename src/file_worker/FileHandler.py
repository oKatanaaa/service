import os
from multiprocessing import Queue
from threading import Thread

from cluster_worker.TableRow import TableRow
from file_worker.ContentGrubber import ContentGrubber
from file_worker.DirWatcher import DirWatcher
from geometry.Point import Point


class FileHandler(Thread):
    WORKING_DIRS = list()

    ACTIONS = {
        1: "Created",
        2: "Deleted",
        3: "Updated",
        4: "Renamed from",
        5: "Renamed to"
    }

    def __init__(self, target_path: str, is_teacher: bool, job_queue: Queue):
        super().__init__()
        self.setName("File Thread " + str(self.ident))
        self.setDaemon(True)
        self.target_path = target_path
        self.is_teacher = is_teacher
        self.WORKING_DIRS.append({'path': target_path,
                                  'teacher': is_teacher})

        self.job_queue = job_queue
        self.content_grubber = ContentGrubber("image")
        self.queue = Queue()
        self.dir_watcher = DirWatcher(self.target_path, self.queue)
        pass

    def run(self):
        self.dir_watcher.start()
        old_name = None

        while True:
            changes = self.queue.get()

            for action, filename in changes:
                if filename is None or filename[-4:] != '.tif':
                    continue
                full_filename = os.path.join(self.target_path, filename)

                if action == 2 or os.path.getsize(full_filename) != 0:
                    if action == 1:
                        pass

                    elif action == 2:
                        row = TableRow(filename, None)
                        self.job_queue.put({
                            "teacher": self.is_teacher,
                            "action": action,
                            "row": row
                        })
                        pass

                    elif action == 3:
                        point = Point(self.content_grubber.get_info(full_filename))
                        row = TableRow(filename, point)
                        self.job_queue.put({
                            "teacher": self.is_teacher,
                            "action": action,
                            "row": row
                        })
                        pass

                    elif action == 4:
                        old_name = full_filename
                        pass

                    elif action == 5:
                        self.job_queue.put({
                            "teacher": self.is_teacher,
                            "action": action,
                            "old_name": old_name,
                            "new_name": full_filename,
                            "row": None
                        })
                        pass

                    else:
                        raise Exception("Unsupported Operation")
