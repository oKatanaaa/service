import os
import shutil
import sys
from multiprocessing import Queue
from shutil import copyfile
from timeit import default_timer as timer

from DistributionSystem import DistributionSystem

if __name__ == "__main__":
    test_path = r"Q:\service\test\picture_test"

    # noinspection PyBroadException
    try:

        time_list = list()
        start_time = timer()
        t_start = start_time

        teach_path = sys.argv[1]
        analyze_path = sys.argv[2]
        first_option = sys.argv[3]
        second_option = sys.argv[4]
        third_option = sys.argv[5]
        fourth_option = sys.argv[6]
        algorithm = sys.argv[7]

        service = Queue()
        distSystem = DistributionSystem(teach_path, analyze_path, 'nearest_neighbours', service)
        distSystem.start()

        f_time = timer()
        time_list.append(f_time - start_time)
        start_time = f_time

        for f in os.listdir(test_path + r"\Group " + first_option):
            shutil.copy(os.path.join(test_path + r"\Group " + first_option, f),
                        os.path.join(test_path + r"\cls", f))

        service.get()

        f_time = timer()
        time_list.append(f_time - start_time)
        start_time = f_time

        for f in os.listdir(test_path + r"\CopyFrom"):
            shutil.copy(os.path.join(test_path + r"\CopyFrom", f),
                        os.path.join(test_path + r"\for_identify", f))

        service.get()

        f_time = timer()
        time_list.append(f_time - start_time)
        start_time = f_time

        for f in os.listdir(test_path + r"\Group " + second_option):
            shutil.copy(os.path.join(test_path + r"\Group " + second_option, f),
                        os.path.join(test_path + r"\cls", f))

        service.get()

        f_time = timer()
        time_list.append(f_time - start_time)
        start_time = f_time

        for f in os.listdir(test_path + r"\Group " + third_option):
            shutil.copy(os.path.join(test_path + r"\Group " + third_option, f),
                        os.path.join(test_path + r"\cls", f))

        service.get()

        f_time = timer()
        time_list.append(f_time - start_time)
        start_time = f_time

        for f in os.listdir(test_path + r"\Group " + fourth_option):
            os.remove(os.path.join(test_path + r"\cls", f))

        service.get()

        f_time = timer()
        time_list.append(f_time - start_time)
        start_time = f_time

        for f in os.listdir(test_path + r"\cls"):
            os.remove(os.path.join(test_path + r"\cls", f))

        for f in os.listdir(test_path + r"\for_identify"):
            os.remove(os.path.join(test_path + r"\for_identify", f))

        # with open('full_results.txt', 'a') as file:
        #     file.write(str(time_list) + '\n')

        print(str(time_list))
        exit(str(time_list))

    except Exception:
        pass
