import os
import shutil
import sys
from shutil import copyfile
from timeit import default_timer as timer

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
    signal = False
    time_lst = []
    start_time = timer()
    t_start = start_time

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
    # Utility and interrupt queue
    message_system.root_register()
    th.start()
    ch.start()
    fh.start()
    first_watcher.start()
    second_watcher.start()

    # Test
    f_time = timer()
    time_lst.append(f_time - start_time)
    start_time = f_time

    for f in os.listdir(r"C:\Users\Banayaki\PycharmProjects\service\test\Group A"):
        shutil.copy(os.path.join(r"C:\Users\Banayaki\PycharmProjects\service\test\Group A", f),
                    os.path.join(r"C:\Users\Banayaki\PycharmProjects\service\test\cls", f))

    message_system.queue_listing[message_system.ADDRESS_LIST['Utility']][0].get()

    f_time = timer()
    time_lst.append(f_time - start_time)
    start_time = f_time

    for f in os.listdir(r"C:\Users\Banayaki\PycharmProjects\service\test\File group"):
        shutil.copy(os.path.join(r"C:\Users\Banayaki\PycharmProjects\service\test\File group", f),
                    os.path.join(r"C:\Users\Banayaki\PycharmProjects\service\test\for_identify", f))

    message_system.queue_listing[message_system.ADDRESS_LIST['Utility']][0].get()

    f_time = timer()
    time_lst.append(f_time - start_time)
    start_time = f_time

    for f in os.listdir(r"C:\Users\Banayaki\PycharmProjects\service\test\Group C"):
        shutil.copy(os.path.join(r"C:\Users\Banayaki\PycharmProjects\service\test\Group C", f),
                    os.path.join(r"C:\Users\Banayaki\PycharmProjects\service\test\cls", f))

    message_system.queue_listing[message_system.ADDRESS_LIST['Utility']][0].get()

    f_time = timer()
    time_lst.append(f_time - start_time)
    start_time = f_time

    for f in os.listdir(r"C:\Users\Banayaki\PycharmProjects\service\test\Group B"):
        shutil.copy(os.path.join(r"C:\Users\Banayaki\PycharmProjects\service\test\Group B", f),
                    os.path.join(r"C:\Users\Banayaki\PycharmProjects\service\test\cls", f))

    message_system.queue_listing[message_system.ADDRESS_LIST['Utility']][0].get()

    f_time = timer()
    time_lst.append(f_time - start_time)
    start_time = f_time

    for f in os.listdir(r"C:\Users\Banayaki\PycharmProjects\service\test\Group B"):
        os.remove(os.path.join(r"C:\Users\Banayaki\PycharmProjects\service\test\cls", f))

    message_system.queue_listing[message_system.ADDRESS_LIST['Utility']][0].get()

    f_time = timer()
    time_lst.append(f_time - start_time)
    start_time = f_time

    print(time_lst)

    for f in os.listdir(r"C:\Users\Banayaki\PycharmProjects\service\test\cls"):
        os.remove(os.path.join(r"C:\Users\Banayaki\PycharmProjects\service\test\cls", f))

    for f in os.listdir(r"C:\Users\Banayaki\PycharmProjects\service\test\for_identify"):
        os.remove(os.path.join(r"C:\Users\Banayaki\PycharmProjects\service\test\for_identify", f))
