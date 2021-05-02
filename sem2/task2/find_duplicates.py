from mrjob.job import MRJob 
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol
import json
import os
import re
from math import ceil
from decimal import Decimal
import Levenshtein

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
            self.split(partition_file, len(data["Заведения по адресам"]), "bars")
        with open(os.path.join(INPUT_DATA_DIR, DATA_COWORKS), "r") as file:
            data = json.load(file)
            self.split(partition_file, len(data["Заведения по адресам"]), "coworkings")

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
                return data["Заведения по адресам"][range_idxs[0]:range_idxs[1]]
        with open(self.data_coworkings, "r", encoding="utf-8") as file:
            data = json.load(file);
            return data["Заведения по адресам"][range_idxs[0]:range_idxs[1]]

        
    def num_distance(self, num1, num2):
        return abs(num1 - num2) * 10**6
        
    def string_distance(self, str1, str2):
        return Levenshtein.distance(str1.lower(), str2.lower()) / len(str1 + str2)
        
    def phone_distance(self, str1, str2):
        return int(Levenshtein.distance(str1, str2) > 3)
        
    def similarity(self, item1, item2):
        similarity_scores = {"GeoData": 1.0, "ContactPhone": 1.0, "Address": 1.0}
        similarity_ratio = {"GeoData": 0.8, "ContactPhone": 0.05, "District": 0.05, "AdmArea": 0.05, "Address": 0.05}
        
        dist1 = 0.0
        if (item1["GeoData"]["Latitude_WGS84"] != "U") and (item2["GeoData"]["Latitude_WGS84"] != "U"):
            dist1 = self.num_distance(Decimal(item1["GeoData"]["Latitude_WGS84"]), Decimal(item2["GeoData"]["Latitude_WGS84"]))
        dist2 = 0.0
        if (item1["GeoData"]["Longitude_WGS84"] != "U") and (item2["GeoData"]["Longitude_WGS84"] != "U"):
            dist2 = self.num_distance(Decimal(item1["GeoData"]["Longitude_WGS84"]), Decimal(item2["GeoData"]["Longitude_WGS84"]))
        score = 1.0 - float(dist1) - float(dist2)
        similarity_scores["GeoData"] = score if score > 0 else 0.0
        
        dist = 0.0
        if (item1["Address"] != "U") and (item2["Address"] != "U"):
            dist = self.string_distance(item1["Address"], item2["Address"])
        score = 1.0 - dist
        similarity_scores["Address"] = score if score > 0 else 0.0
        
        dist = 0.0
        if (item1["District"] != "U") and (item2["District"] != "U"):
            dist = self.string_distance(item1["District"], item2["District"])
        score = 1.0 - dist
        similarity_scores["District"] = score if score > 0 else 0.0
        
        dist = 0.0
        if (item1["AdmArea"] != "U") and (item2["AdmArea"] != "U"):
            dist = self.string_distance(item1["AdmArea"], item2["AdmArea"])
        score = 1.0 - dist
        similarity_scores["AdmArea"] = score if score > 0 else 0.0
            
        dist = 0.0
        if (item1["ContactPhone"] != "U") and (item2["ContactPhone"] != "U"):
            dist = self.phone_distance(item1["ContactPhone"], item2["ContactPhone"])
        score = 1.0 - dist
        similarity_scores["ContactPhone"] = score if score > 0 else 0
        
        similarity = 0.0
        for key in similarity_scores.keys():
            similarity += similarity_scores[key] * similarity_ratio[key]
        return similarity
        
        
    def get_dups(self, item):
        dups = []
        with open(self.data_bars, "r") as file:
            data = json.load(file)
            for data_item in data["Заведения по адресам"]:
                if len(data_item.keys()) < 3:
                    continue
                if (self.similarity(item, data_item) > 0.69) and (data_item["global_id"] != item["global_id"]):
                    dups.append(data_item)
        with open(self.data_coworkings, "r") as file:
            data = json.load(file)
            for data_item in data["Заведения по адресам"]:
                if len(data_item.keys()) < 3:
                    continue
                if (self.similarity(item, data_item) > 0.69) and (data_item["global_id"] != item["global_id"]):
                    dups.append(data_item)
        dups.append(item)
        return dups

    def get_target_schema(self):
        json_file = open(os.path.join(DATA_SCHEMA_DIR, SCHEMA), "r", encoding="utf-8")
        schema = json.load(json_file)
        return schema["items"]["properties"]
        


class DupsFind(MRJob):
    zones = os.path.join(OUTPUT_DATA_DIR, ZONE_FILE)
    target_schema = os.path.join(DATA_SCHEMA_DIR, SCHEMA)
    helper = Helper()
    target_schema = helper.get_target_schema()
    
    def start(self):
        self.helper.split_data()
        with open(self.zones, "w", encoding="utf-8") as json_file:
            json_file.write("[\n")
            
    def end(self):
        with open(self.zones, "a", encoding="utf-8") as json_file:
            json_file.write("{\n\t\"Region\": \"Moscow\"\n}\n]")
        
    def zone_mapper(self, _, line):
        range_idxs = []
        for word in WORD_RE.findall(line):
            range_idxs.append(word)
        range_idxs[0] = int(range_idxs[0])
        range_idxs[1] = int(range_idxs[1])
    
        for item in self.helper.generate_items(range_idxs):
            if len(item.keys()) < 3:
                continue
            primary_key = 0
            dups = self.helper.get_dups(item)
            for dup in dups:
                primary_key += int(dup["global_id"])
            yield primary_key, dups
            
    def zone_reducer(self, id, duplicates):
        items = []
        for dups in duplicates:
            items = dups

        if len(items) < 2:
            return
        with open(self.zones, "a", encoding="utf-8") as json_file:
            json_file.write("{\n")
            json_file.write("\t\"Name\": \"Zone\",\n")
            json_file.write("\t\"id\": " + str(id) + ",\n")
            json_file.write("\t\"properties\":\n\t[\n")
            for value in items[:-1]:
                self.dump_zone_value(json_file, value, ",")
            self.dump_zone_value(json_file, items[-1], "")
            json_file.write("\t]\n},\n")
            
            
    def dump_zone_value(self, json_file, value, last):
        json_file.write("\t\t{\n")
        if len(value) > 1:
            keys = list(value.keys())
            for key in keys[:-1]:
                self.dump(json_file, key, value[key], ",\n")
            self.dump(json_file, keys[-1], value[keys[-1]], "\n")
            json_file.write("\t\t}" + last + "\n")
                
    def dump(self, json_file, key, value, last):
        if isinstance(value, dict):
            temp_keys = list(value.keys())
            if self.target_schema[key]["type"] != "DICTIONARY":
                json_file.write("\t\t\t\"" + key + "\": \"" + value[temp_keys[0]] + "\"\n")
            else:
                json_file.write("\t\t\t\"" + key + "\": \n\t\t\t{\n")
                end = ","
                for i in range(len(temp_keys)):
                    if i == len(temp_keys) - 1:
                        end = ""
                    k = temp_keys[i]
                    v = ",".join(value[k]) if isinstance(value[k], list) else value[k]
                    json_file.write("\t\t\t\t\"" + k + "\": \"" + v +"\"" + end + "\n")
                json_file.write("\t\t\t}" + last)
        elif isinstance(value, list):
            json_file.write("\t\t\t\"" + key + "\": \n\t\t\t[\n")
            end = ","
            for i in range(len(value)):
                if i == len(value) - 1:
                    end = ""
                temp_keys = list(value[i].keys())
                json_file.write("\t\t\t\t{\n")
                for k in temp_keys[:-1]:
                    json_file.write("\t\t\t\t\t\"" + k + "\": \"" + value[i][k] +"\",\n")
                if len(temp_keys) > 0:
                    json_file.write("\t\t\t\t\t\"" + temp_keys[-1] + "\": \"" + value[i][temp_keys[-1]] +"\"\n")
                json_file.write("\t\t\t\t}" + end + "\n")
            json_file.write("\t\t\t]" + last)
        else:
            json_file.write("\t\t\t\"" + key + "\": \"" + value + "\"" + last)
                
    def steps(self):
        return [
            MRStep(mapper=self.zone_mapper,
                   reducer=self.zone_reducer)
        ]


if __name__ == '__main__':

    duplicates_finder = DupsFind()
    duplicates_finder.start()
    duplicates_finder.run()
    duplicates_finder.end()
