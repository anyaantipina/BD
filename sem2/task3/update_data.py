import os

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
SOURCE_DATA_DIR = os.path.join(SCRIPT_DIR, "..", "task1", "out")
DUPS_DATA_DIR = os.path.join(SCRIPT_DIR, "..", "task2", "out")
TARGET_DATA_DIR = os.path.join(SCRIPT_DIR, "data")

os.system("rm -rf " + TARGET_DATA_DIR + "/*")
os.system("cp -r " + SOURCE_DATA_DIR + "/* " + TARGET_DATA_DIR)
os.system("cp -r " + DUPS_DATA_DIR + "/* " + TARGET_DATA_DIR)

