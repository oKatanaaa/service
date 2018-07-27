import logging
import os
import sys
from shutil import copyfile

from DirInspector import DirInspector

# Log-control block
if os.path.exists(str(os.getcwd()) + ".\logs\my_watch.log") and os.path.getsize(".\logs\my_watch.log") > 100240:
    copyfile(".\logs\my_watch.log", ".\logs\old_my_watch.log")
    os.remove(str(os.getcwd()) + ".\logs\my_watch.log")

# Logger initialization.
logger = logging.getLogger("Main")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(".\logs\my_watch.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s -> %(message)s'))
logger.addHandler(file_handler)


def check_args():
    if len(sys.argv) != 2:
        logger.error("Have not enough args, except one: dir_for_watching \n"
                     "Launch script again, and don't forget about args")
        sys.exit(-1)
        pass
    if os.path.exists(sys.argv[1]) is False:
        logger.error("Directory does not exist, Launch script again, and don't forget about args")
        sys.exit(-1)
        pass
    logger.info('My watch begin. Watching for ' + str(sys.argv[1]))
    return sys.argv[1]


if __name__ == "__main__":
    root_path = check_args()
    main_class = DirInspector(root_path)
