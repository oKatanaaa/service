import os
import shutil
import sys
from shutil import copyfile
from timeit import default_timer as timer

from picture_analyze_service.handlers.ClusterHandler import ClusterHandler
from picture_analyze_service.handlers.FileHandler import FileHandler
from picture_analyze_service.handlers.TableHandler import TableHandler
from picture_analyze_service.handlers.Watcher import Watcher
from messageSystem.Message import Message
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
    time_lst = []
    start_time = timer()
    t_start = start_time

    if os.path.exists(str(os.getcwd()) + ".\logs\my_watch.log"):
        copyfile(".\logs\my_watch.log", ".\logs\old_my_watch.log")
        os.remove(str(os.getcwd()) + ".\logs\my_watch.log")

    # teach_path, analyze_path = check_args()

    teach_path = sys.argv[1]
    analyze_path = sys.argv[2]
    first_option = sys.argv[3]
    second_option = sys.argv[4]
    third_option = sys.argv[5]
    fourth_option = sys.argv[6]
    delete_type = sys.argv[7]

    message_system = MessageSystem()
    first_watcher = Watcher(message_system, teach_path, "teach_table.csv", True)
    message_system.register(first_watcher)
    second_watcher = Watcher(message_system, analyze_path, "table.csv")
    # message_system.register(second_watcher)
    fh = FileHandler(message_system)
    message_system.register(fh)
    if delete_type != "no ngh":
        ch = ClusterHandler(message_system, delete_type)
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

    for f in os.listdir(r"..\picture_test\Group " + first_option):
        shutil.copy(os.path.join(r"..\picture_test\Group " + first_option, f),
                    os.path.join(r"..\picture_test\cls", f))

    message_system.queue_listing[message_system.ADDRESS_LIST['Utility']][0].get()

    f_time = timer()
    time_lst.append(f_time - start_time)
    start_time = f_time

    for f in os.listdir(r"..\picture_test\CopyFrom"):
        shutil.copy(os.path.join(r"..\picture_test\CopyFrom", f),
                    os.path.join(r"..\picture_test\for_identify", f))

    message_system.queue_listing[message_system.ADDRESS_LIST['Utility']][0].get()

    f_time = timer()
    time_lst.append(f_time - start_time)
    start_time = f_time

    for f in os.listdir(r"..\picture_test\Group " + second_option):
        shutil.copy(os.path.join(r"..\picture_test\Group " + second_option, f),
                    os.path.join(r"..\picture_test\cls", f))

    message_system.queue_listing[message_system.ADDRESS_LIST['Utility']][0].get()

    f_time = timer()
    time_lst.append(f_time - start_time)
    start_time = f_time

    for f in os.listdir(r"..\picture_test\Group " + third_option):
        shutil.copy(os.path.join(r"..\picture_test\Group " + third_option, f),
                    os.path.join(r"..\picture_test\cls", f))

    message_system.queue_listing[message_system.ADDRESS_LIST['Utility']][0].get()

    f_time = timer()
    time_lst.append(f_time - start_time)
    start_time = f_time

    for f in os.listdir(r"..\picture_test\Group " + fourth_option):
        os.remove(os.path.join(r"..\picture_test\cls", f))

    message_system.queue_listing[message_system.ADDRESS_LIST['Utility']][0].get()

    new_msg = Message(
        "Main",
        message_system.ADDRESS_LIST['FileHandler'],
        {'option': 'kill'}
    )
    message_system.send(new_msg)
    new_msg = Message(
        "Main",
        message_system.ADDRESS_LIST['ClusterHandler'],
        {'option': 'kill'}
    )
    message_system.send(new_msg)
    new_msg = Message(
        "Main",
        message_system.ADDRESS_LIST['TableHandler'],
        {'option': 'kill'}
    )
    message_system.send(new_msg)
    new_msg = Message(
        "Main",
        message_system.ADDRESS_LIST['Watcher'],
        {'option': 'kill'}
    )
    message_system.send(new_msg)

    f_time = timer()
    time_lst.append(f_time - start_time)
    start_time = f_time

    for f in os.listdir(r"..\picture_test\cls"):
        os.remove(os.path.join(r"..\picture_test\cls", f))

    for f in os.listdir(r"..\picture_test\for_identify"):
        os.remove(os.path.join(r"..\picture_test\for_identify", f))

    # with open('full_results.txt', 'a') as file:
    #     file.write(str(time_lst) + '\n')

    print(str(time_lst))
    exit(str(time_lst))
