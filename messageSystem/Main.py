import os
import sys
from shutil import copyfile

from handlers.ClusterHandler import ClusterHandler
from handlers.FileHandler import FileHandler
from handlers.TableHandler import TableHandler
from handlers.Watcher import Watcher
from messageSystem.MessageSystem import MessageSystem


def check_args():
    if len(sys.argv) != 3:
        sys.exit(-1)
        pass
    if os.path.exists(sys.argv[1]) is False or os.path.exists(sys.argv[2]) is False:
        sys.exit(-1)
        pass
    return sys.argv[1], sys.argv[2]


if __name__ == "__main__":
    if os.path.exists(str(os.getcwd()) + ".\logs\my_watch.log") and os.path.getsize(".\logs\my_watch.log") > 100240:
        copyfile(".\logs\my_watch.log", ".\logs\old_my_watch.log")
        os.remove(str(os.getcwd()) + ".\logs\my_watch.log")
    teach_path, analyze_path = check_args()
    message_system = MessageSystem()
    first_watcher = Watcher(message_system, teach_path, "teach_table.csv", True)
    message_system.register(first_watcher)
    second_watcher = Watcher(message_system, analyze_path, "table.csv")
    # message_system.register(second_watcher)
    fh = FileHandler(message_system)
    message_system.register(fh)
    ch = ClusterHandler(message_system)
    message_system.register(ch)
    th = TableHandler(message_system)
    message_system.register(th)
    th.start()
    ch.start()
    fh.start()
    first_watcher.start()
    second_watcher.start()
