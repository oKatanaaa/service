import os
import subprocess
from statistics import mean

if __name__ == "__main__":
    first_list = ['A', 'A', 'A', 'A', 'A', 'A',
                  'B', 'B', 'B', 'B', 'B', 'B',
                  'C', 'C', 'C', 'C', 'C', 'C']
    second_list = ['B', 'B', 'B', 'C', 'C', 'C', 'A', 'A', 'A',
                   'C', 'C', 'C', 'A', 'A', 'A', 'B', 'B', 'B']
    third_list = ['C', 'C', 'C', 'B', 'B', 'B', 'C', 'C', 'C',
                  'A', 'A', 'A', 'B', 'B', 'B', 'A', 'A', 'A']
    fourth_list = ['A', 'B', 'C']
    algorithms = ['nearest_neighbours', 'distance_variation', 'neighbour_relations_my_delete']
    epochs = 10
    number_of_options = 18
    test_path = r"Q:/service/test/picture_test"

    for option in range(0, number_of_options):
        results = [list(), list(), list(), list(), list(), list()]
        first_option = first_list[option]
        second_option = second_list[option]
        third_option = third_list[option]
        fourth_option = fourth_list[option % 3]

        for epoch in range(epochs):
            string = None
            # noinspection PyBroadException
            try:
                with subprocess.Popen([r'C:/Users/Banayaki/Anaconda3/python.exe',
                                       r'Q:/service/src/Executor.py',
                                       test_path + r'/cls',
                                       test_path + r'/for_identify',
                                       first_option,
                                       second_option,
                                       third_option,
                                       fourth_option,
                                       algorithms[1]
                                       algorithms[2]
                                       ], stdout=subprocess.PIPE) as process:

                    string = str(process.stdout.readline())
                    process.kill()
            except Exception:
                for f in os.listdir(test_path + r"\cls"):
                    os.remove(os.path.join(test_path + r"\cls", f))

                for f in os.listdir(test_path + r"\for_identify"):
                    os.remove(os.path.join(test_path + r"\for_identify", f))
                epoch -= 1
            if string is not None:
                string = string[3:-6]
                print(string)
                string = string.replace(',', '')
                string = string.replace('\'', '')
                string = string.split(' ')
                results[0].append(float(string[0]))
                results[1].append(float(string[1]))
                results[2].append(float(string[2]))
                results[3].append(float(string[3]))
                results[4].append(float(string[4]))
                results[5].append(float(string[5]))

        with open('results.txt', 'a') as file:
            file.write("Набор опций №" + str(option) + ": "
                       + first_option + ", " + second_option + ", " + third_option + ", " + fourth_option + "\n")
            file.write("Средние значения затраченного времени.\n")
            file.write(str(mean(results[0])) + " - приложение запущено\n")
            file.write(str(mean(results[1]))
                       + " - загружена группа " + str(first_option) + " и сформирован список классификаторов\n")
            file.write(str(mean(results[2])) + " - загружен список файлов и определены их кластеры\n")
            file.write(str(mean(results[3])) + " - загружена группа " + str(second_option)
                       + " и переопределены кластеры\n")
            file.write(str(mean(results[4])) + " - загружена группа " + str(third_option)
                       + " и переопределенны кластеры\n")
            file.write(str(mean(results[5])) + " - удалена группа " + str(fourth_option)
                       + " и перепопределены кластеры\n")
exit(1)
