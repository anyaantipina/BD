import os

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
SOURCE_DATA_DIR = os.path.join(SCRIPT_DIR, "..", "task1", "out")
SCHEMA_FILE = os.path.join(SCRIPT_DIR, "..", "task1", "schema", "target-schema.json")
TARGET_SCHEMA_DIR = os.path.join(SCRIPT_DIR, "schema")
TARGET_DATA_DIR = os.path.join(SCRIPT_DIR, "data")

os.system("rm -rf " + TARGET_DATA_DIR + "/*")
os.system("cp -r " + SOURCE_DATA_DIR + "/* " + TARGET_DATA_DIR)

os.system("rm -rf " + TARGET_SCHEMA_DIR + "/*")
os.system("cp " + SCHEMA_FILE  + " " + TARGET_SCHEMA_DIR)
