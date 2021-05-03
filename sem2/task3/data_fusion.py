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
DATA_DUPS = "zones.json"

class Helper():
    data_bars = os.path.join(INPUT_DATA_DIR, DATA_BARS)
    data_coworkings = os.path.join(INPUT_DATA_DIR, DATA_COWORKS)
    data_dups = os.path.join(INPUT_DATA_DIR, DATA_DUPS)
    street_synonyms = ["улица", "площадь", "проспект", "бульвар", "переулок", "шоссе", "набережная"]
    building_synonyms = ["дом", "владение"]
    microdistr_synonyms = ["микрорайон", "деревня"]
    address_items = ["AdmArea", "City", "District", "Microdistrict", "Street", "Building"]
    
    
    def split_data(self):
        partition_file = open(os.path.join(INTERNAL_DIR, INTERNAL_FILE), "w")
        with open(os.path.join(INPUT_DATA_DIR, DATA_BARS), "r") as file:
            data = json.load(file)
            self.split(partition_file, len(data["Заведения по адресам"]), "bars")
        with open(os.path.join(INPUT_DATA_DIR, DATA_COWORKS), "r") as file:
            data = json.load(file)
            self.split(partition_file, len(data["Заведения по адресам"]), "coworkings")
        with open(os.path.join(INPUT_DATA_DIR, DATA_DUPS), "r") as file:
            data = json.load(file)
            self.split(partition_file, len(data), "zones")

        partition_file.close()
        
    def split(self, partition_file, count, format):
        per_mapper = ceil(count / 10)
        i = 0
        while i < count:
            start = i
            end = i + per_mapper if (i + per_mapper <= count) else count
            partition_file.write("%s %s %s" % (start, end, format))
            i = end
            if i < count:
                partition_file.write("\n")
        partition_file.write("\n")
                
    def generate_items(self, range_idxs):
        if range_idxs[2] == "bars":
            with open(self.data_bars, "r", encoding="utf-8") as file:
                data = json.load(file);
                return data["Заведения по адресам"][range_idxs[0]:range_idxs[1]]
        if range_idxs[2] == "coworkings":
            with open(self.data_coworkings, "r", encoding="utf-8") as file:
                data = json.load(file);
                return data["Заведения по адресам"][range_idxs[0]:range_idxs[1]]
        with open(self.data_dups, "r", encoding="utf-8") as file:
            data = json.load(file);
            return data[range_idxs[0]:range_idxs[1]]
            

    def is_conflict(self, item):
        with open(self.data_dups, "r") as dups_file:
            data = json.load(dups_file)
            for zone in data:
                if len(zone.keys()) < 2:
                    continue
                for dup_item in zone["properties"]:
                    if item["global_id"] == dup_item["global_id"]:
                        return True
        return False
                        
    def resolve_geoData(self, zone):
        mean_lat = 0
        mean_long = 0
        for item in zone:
            mean_lat += float(item["GeoData"]["Latitude_WGS84"])
            mean_long += float(item["GeoData"]["Longitude_WGS84"])
        mean_lat = float(mean_lat / len(zone))
        mean_long = float(mean_long / len(zone))
        return {"Latitude_WGS84": mean_lat, "Longitude_WGS84": mean_long}

    def get_canonical_string(self, str):
        if str[0] == " ":
            return str[1:]
        return str

    def extract_addr(self, item):
        address = {"AdmArea": item["AdmArea"], "District": item["District"]}
        
        for addr_field in item["Address"].split(","):
            if "город" in addr_field:
                address["City"] = self.get_canonical_string(addr_field)
                continue
            else:
                address["City"] = "город Москва"
                
            if "корпус" in addr_field or "строение" in addr_field:
                if not "Building" in address.keys():
                    address["Building"] = self.get_canonical_string(addr_field)
                else:
                    address["Building"] += addr_field
                continue
                
            for syn in self.street_synonyms:
                if syn in addr_field:
                    address["Street"] = self.get_canonical_string(addr_field)
                    break
            for syn in self.building_synonyms:
                if syn in addr_field:
                    address["Building"] = self.get_canonical_string(addr_field)
                    break
            for syn in self.microdistr_synonyms:
                if syn in addr_field:
                    address["Microdistrict"] = self.get_canonical_string(addr_field)
                    break
        for addr_item in self.address_items:
            if not addr_item in address.keys():
                address[addr_item] = "U"
        return address
        
    def get_reliability(self, item, addr_item):
        if addr_item == "U":
            return 0.0
        if item["ContactPhone"] == "U" or item["ContactPhone"] == "нет телефона":
            return 0.5
        return 1.0
        
    def string_distance(self, str1, str2):
        return Levenshtein.distance(str1.lower(), str2.lower()) / len(str1 + str2)
        
    def get_more_reliable(self, addr_items):
        more_reliable_set = set()
        max_reliability = 0.0
        for addr_item in addr_items:
            if addr_item["reliability"] > max_reliability:
                max_reliability = addr_item["reliability"]
                more_reliable_set.clear()
                more_reliable_set.add(addr_item["item"])
                continue
            for processed_item in more_reliable_set:
                if self.string_distance(processed_item, addr_item["item"]) > 0.03:
                    more_reliable_set.add(addr_item["item"])
                    break
                
        result_item = ""
        for item in more_reliable_set:
            if result_item == "":
                result_item = item
                continue
            result_item += ", " + self.get_canonical_string(item)
            
        return result_item
        
    def resolve_addr(self, zone):
        resolved_address = {}
        addresses = {}

        for item in zone:
            address = self.extract_addr(item)
            for addr_item in address.keys():
                if not addr_item in addresses:
                    addresses[addr_item] = [{"item": address[addr_item], "reliability": self.get_reliability(item, address[addr_item])}]
                else:
                    addresses[addr_item].append({"item": address[addr_item], "reliability": self.get_reliability(item, address[addr_item])})
    
        for addr_item in self.address_items:
            if not addr_item in addresses.keys():
                resolved_address[addr_item] = "U"
                continue
            resolved_address[addr_item] = self.get_more_reliable(addresses[addr_item])

        return resolved_address
        
    def merge_establishments(self, zone):
        establishments = []
        for item in zone:
            establishments.append({"Class": item["Class"], "Name": item["Name"], "ContactPhone": item["ContactPhone"], "global_id": item["global_id"]})
        return establishments
            
                        
    def resolve_conflict(self, zone):
        resolve_item = dict()
        resolve_item["GeoData"] = self.resolve_geoData(zone)
        resolve_item["Address"] = self.resolve_addr(zone)
        resolve_item["Establishments"] = self.merge_establishments(zone)
        resolve_item["address_id"] = self.resolve_addrid(zone)
        return resolve_item
        
    def resolve_addrid(self, zone):
        addr_id = 0
        for item in zone:
            addr_id += int(item["global_id"])
        return self.get_address_id(addr_id)

    def get_address_id(self, addr_id):
        return abs(hash(addr_id))
        
    def convert_item(self, item):
        convert_item = dict()
        convert_item["GeoData"] = item["GeoData"]
        convert_item["Address"] = self.extract_addr(item)
        convert_item["Establishments"] = self.merge_establishments([item])
        convert_item["address_id"] = self.get_address_id(item["global_id"])
        return convert_item

    def get_target_schema(self):
        json_file = open(os.path.join(DATA_SCHEMA_DIR, SCHEMA), "r", encoding="utf-8")
        schema = json.load(json_file)
        return schema["items"]["properties"]
        


class DataFusion(MRJob):
    target_schema = os.path.join(DATA_SCHEMA_DIR, SCHEMA)
    out_file = os.path.join(OUTPUT_DATA_DIR, "out_data.json")
    helper = Helper()
    target_schema = helper.get_target_schema()
    
    def start(self):
        self.helper.split_data()
        with open(self.out_file, "w", encoding="utf-8") as json_file:
            json_file.write("{\n\"Заведения по адресам\":\n[\n")
            
    def end(self):
        with open(self.out_file, "a", encoding="utf-8") as json_file:
            json_file.write("\t{\n\t\"Region\": \"Москва\"\n\t}\n]\n}")
        
    def fusion_mapper(self, _, line):
        range_idxs = []
        for word in WORD_RE.findall(line):
            range_idxs.append(word)
        range_idxs[0] = int(range_idxs[0])
        range_idxs[1] = int(range_idxs[1])
    
        for item in self.helper.generate_items(range_idxs):
            if len(item.keys()) < 2:
                continue
            
            if item["Name"] == "Zone":
                resolve_item = self.helper.resolve_conflict(item["properties"])
                yield resolve_item["address_id"], resolve_item
                continue
                
            if self.helper.is_conflict(item):
                continue
            
            convert_item = self.helper.convert_item(item)
            yield convert_item["address_id"], convert_item
            
    def fusion_reducer(self, id, items):
        list_items = []
        for item in items:
            list_items.append(item)
        with open(self.out_file, "a", encoding="utf-8") as json_file:
            json_file.write("\t{\n")
            target_keys = list(list_items[0].keys())
            for key in target_keys[:-1]:
                self.dump(json_file, key, list_items[0][key], ",")
            self.dump(json_file, target_keys[-1], list_items[0][target_keys[-1]], "")
            json_file.write("\t},\n")

                
    def dump(self, json_file, key, value, last):
        if isinstance(value, dict):
            temp_keys = list(value.keys())
            if self.target_schema[key]["type"] != "DICTIONARY":
                json_file.write("\t\t\"" + key + "\": \"" + str(value[temp_keys[0]]) + "\"\n")
            else:
                json_file.write("\t\t\"" + key + "\": \n\t\t{\n")
                end = ","
                for i in range(len(temp_keys)):
                    if i == len(temp_keys) - 1:
                        end = ""
                    k = temp_keys[i]
                    v = ",".join(value[k]) if isinstance(value[k], list) else value[k]
                    json_file.write("\t\t\t\"" + k + "\": \"" + str(v) +"\"" + end + "\n")
                json_file.write("\t\t}" + last + "\n")
        elif isinstance(value, list):
            json_file.write("\t\t\"" + key + "\": \n\t\t[\n")
            end = ","
            for i in range(len(value)):
                if i == len(value) - 1:
                    end = ""
                temp_keys = list(value[i].keys())
                json_file.write("\t\t\t{\n")
                for k in temp_keys[:-1]:
                    json_file.write("\t\t\t\t\"" + k + "\": \"" + value[i][k] +"\",\n")
                if len(temp_keys) > 0:
                    json_file.write("\t\t\t\t\"" + temp_keys[-1] + "\": \"" + value[i][temp_keys[-1]] +"\"\n")
                json_file.write("\t\t\t}" + end + "\n")
            json_file.write("\t\t]" + last + "\n")
        else:
            json_file.write("\t\t\"" + key + "\": \"" + str(value) + "\"" + last + "\n")
                
    def steps(self):
        return [
            MRStep(mapper=self.fusion_mapper,
                   reducer=self.fusion_reducer)
        ]


if __name__ == '__main__':

    data_fusion = DataFusion()
    data_fusion.start()
    data_fusion.run()
    data_fusion.end()
