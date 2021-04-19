from mrjob.job import MRJob 
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol
import json
import os
import re
from math import ceil
from decimal import Decimal, ROUND_HALF_UP

WORD_RE = re.compile(r"[\w']+")
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
INPUT_DATA_DIR = os.path.join(SCRIPT_DIR, "data")
DATA_SCHEMA_DIR = os.path.join(SCRIPT_DIR, "schema")
INTERNAL_DIR = os.path.join(SCRIPT_DIR, "private")
OUTPUT_DATA_DIR = os.path.join(SCRIPT_DIR, "out")
DATA_BARS = "data-bars.json"
DATA_COWORKS = "data-coworkings.json"
SCHEMA = "target-schema.json"
INTERNAL_FILE = "partition.txt"
ZONE_FILE = "zones.json"

class Helper():
    data_bars = os.path.join(INPUT_DATA_DIR, DATA_BARS)
    data_coworkings = os.path.join(INPUT_DATA_DIR, DATA_COWORKS)

    def split_data(self):
        partition_file = open(os.path.join(INTERNAL_DIR, INTERNAL_FILE), "w")
        
        with open(os.path.join(INPUT_DATA_DIR, DATA_BARS), "r") as file:
            data = json.load(file)
            self.split(partition_file, len(data["Заведения"]), "bars")
        with open(os.path.join(INPUT_DATA_DIR, DATA_COWORKS), "r") as file:
            data = json.load(file)
            self.split(partition_file, len(data["Заведения"]), "coworkings")

        partition_file.close()
        
    def split(self, partition_file, count, format):
        per_mapper = ceil(count / 10)
        i = 0
        while i < count:
            start = i
            end = i + per_mapper if (i + per_mapper <= count) else count
            partition_file.write("%s %s %s" % (start,end, format))
            i = end
            if i < count:
                partition_file.write("\n")
        partition_file.write("\n")
                
    def generate_items(self, range_idxs):
        if range_idxs[2] == "bars":
            with open(self.data_bars, "r", encoding="utf-8") as file:
                data = json.load(file);
                return data["Заведения"][range_idxs[0]:range_idxs[1]]
        with open(self.data_coworkings, "r", encoding="utf-8") as file:
            data = json.load(file);
            return data["Заведения"][range_idxs[0]:range_idxs[1]]
            
    def get_zone(self, item):
        if len(item) == 1:
            return 0, 0
        return Decimal(item["properties"]["GeoData"]["Latitude_WGS84"]).quantize(Decimal('0.000000001'), ROUND_HALF_UP), Decimal(item["properties"]["GeoData"]["Longitude_WGS84"]).quantize(Decimal('0.000000001'), ROUND_HALF_UP)

    def get_target_schema(self):
        json_file = open(os.path.join(DATA_SCHEMA_DIR, SCHEMA), "r", encoding="utf-8")
        schema = json.load(json_file)
        return schema["items"]["properties"]
        


class DumpFind(MRJob):
    zones = os.path.join(OUTPUT_DATA_DIR, ZONE_FILE)
    target_schema = os.path.join(DATA_SCHEMA_DIR, SCHEMA)
    helper = Helper()
    target_schema = helper.get_target_schema()
    
    def start(self):
        self.helper.split_data()
        with open(self.zones, "w", encoding="utf-8") as json_file:
            json_file.write("{\n")
            
    def end(self):
        with open(self.zones, "a", encoding="utf-8") as json_file:
            json_file.write("{\n\t\"Region\": \"Moscow\"\n}\n}")
        
    def zone_mapper(self, _, line):
        range_idxs = []
        for word in WORD_RE.findall(line):
            range_idxs.append(word)
        range_idxs[0] = int(range_idxs[0])
        range_idxs[1] = int(range_idxs[1])
    
        for item in self.helper.generate_items(range_idxs):
            zone = self.helper.get_zone(item)
            if (zone[0] != 0) and (zone[1] != 0):
                yield self.helper.get_zone(item), item
            
    def zone_reducer(self, zone, values):
        items = []
        for value in values:
            items.append(value)
        if len(items) < 2:
            return
        with open(self.zones, "a", encoding="utf-8") as json_file:
            json_file.write("\"Zone\": \n{")
            json_file.write("\t\"Latitude_WGS84\": " + str(zone[0]) + ",\n")
            json_file.write("\t\"Latitude_WGS84\": " + str(zone[1]) + ",\n")
            json_file.write("\t\"items\":\n")
            for value in items[:-1]:
                self.dump_zone_value(json_file, value, ",")
            self.dump_zone_value(json_file, items[-1], "")
            json_file.write("},\n")
            
    def dump_zone_value(self, json_file, value, last):
        json_file.write("\t{\n")
        if len(value) > 1:
            json_file.write("\t\t\"type\": \"" + value["type"] + "\",\n")
            json_file.write("\t\t\"properties\": \n\t\t{\n")
            keys = list(value["properties"].keys())
            for key in keys[:-1]:
                self.dump(json_file, key, value["properties"][key], "\",\n")
            self.dump(json_file, keys[-1], value["properties"][keys[-1]], "\"\n")
            json_file.write("\t\t}\n\t}" + last + "\n")
                
    def dump(self, json_file, key, value, last):
        if isinstance(value, dict):
            temp_keys = list(value.keys())
            if self.target_schema[key]["type"] != "DICTIONARY":
                json_file.write("\t\t\t\"" + key + "\": \"" + value[temp_keys[0]] + "\"\n")
            else:
                json_file.write("\t\t\t\"" + key + "\": \n\t\t\t{\n")
                for k in temp_keys[:-1]:
                    json_file.write("\t\t\t\t\"" + k + "\": \"" + value[k] +"\",\n")
                if len(temp_keys) > 0:
                    if isinstance(value[temp_keys[-1]], list):
                        json_file.write("\t\t\t\t\"" + temp_keys[-1] + "\": \"" + ",".join(value[temp_keys[-1]]) +"\"\n")
                    else:
                        json_file.write("\t\t\t\t\"" + temp_keys[-1] + "\": \"" + value[temp_keys[-1]] +"\"\n")
                json_file.write("\t\t\t},\n")
        else:
            json_file.write("\t\t\t\"" + key + "\": \"" + value + last)
                
    def steps(self):
        return [
            MRStep(mapper=self.zone_mapper,
                   reducer=self.zone_reducer)
        ]


if __name__ == '__main__':

    duplicates_finder = DumpFind()
    duplicates_finder.start()
    duplicates_finder.run()
    duplicates_finder.end()
