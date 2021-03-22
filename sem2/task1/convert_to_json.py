from mrjob.job import MRJob 
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol
import json
import os
import openpyxl
import xml.etree.ElementTree as ET
import re
from math import ceil

WORD_RE = re.compile(r"[\w']+")
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
INPUT_DATA_DIR = os.path.join(SCRIPT_DIR, "data")
DATA_SCHEMA_DIR = os.path.join(SCRIPT_DIR, "schema")
INTERNAL_DIR = os.path.join(SCRIPT_DIR, "private")
OUTPUT_DATA_DIR = os.path.join(SCRIPT_DIR, "out")
XLSX_FILENAME = "data-bars.xlsx"
XML_FILENAME = "data-coworkings.xml"
INTERNAL_FILE = "partition.txt"
TARGET_SCHEMA_FILE = "target-schema.json"

class Helper():

    def split_data(self):
        partition_file = open(os.path.join(INTERNAL_DIR, INTERNAL_FILE), "w")
        
        xlsx_source = openpyxl.load_workbook(os.path.join(INPUT_DATA_DIR, XLSX_FILENAME))
        sheet = xlsx_source.active
        self.split(partition_file, sheet.max_row, "xlsx", 2)
        partition_file.write("\n")
        
        xml_source = ET.parse(os.path.join(INPUT_DATA_DIR, XML_FILENAME))
        root = xml_source.getroot()
        self.split(partition_file, len(list(root)), "xml", 0)

        partition_file.close()
        
    def split(self, partition_file, count, format, start_pos):
        per_mapper = ceil(count / 10)
        i = start_pos
        while i < count:
            start = i
            end = i + per_mapper if (i + per_mapper <= count) else count
            partition_file.write("%s %s %s" % (start,end, format))
            i = end
            if i < count:
                partition_file.write("\n")

    def get_target_schema(self):
        json_file = open(os.path.join(DATA_SCHEMA_DIR, TARGET_SCHEMA_FILE), "r", encoding="utf-8")
        schema = json.load(json_file)
        return schema["items"]["properties"]

    def get_schema_converter(self):
        schema_converter = {}
        schema_converter["PublicPhone"] = "ContactPhone"
        schema_converter["ShortName"] = "Name"
        return schema_converter
        
    def get_schema_types(self):
        schema_types = {}
        schema = self.get_target_schema()
        for key in schema.keys():
            schema_types[key] = schema[key]["type"]
        return schema_types


class ConvertToJSON(MRJob):
    xlsx_source = openpyxl.load_workbook(os.path.join(INPUT_DATA_DIR, XLSX_FILENAME))
    xlsx_json_file = os.path.join(OUTPUT_DATA_DIR, "".join(XLSX_FILENAME.split(".")[:-1]) + ".json")
    xlsx_count = xlsx_source.active.max_row
    
    xml_source = ET.parse(os.path.join(INPUT_DATA_DIR, XML_FILENAME))
    xml_json_file = os.path.join(OUTPUT_DATA_DIR, "".join(XML_FILENAME.split(".")[:-1]) + ".json")
    xml_count = len(list(xml_source.getroot()))
    
    helper = Helper()
    schema_converter = helper.get_schema_converter()
    target_schema = helper.get_target_schema()
    
    def start(self):
        self.helper.split_data()
        self.start_impl(self.xlsx_json_file, "Бары")
        self.start_impl(self.xml_json_file, "Коворкинги")

    def end(self):
        self.end_impl(self.xlsx_json_file, self.xlsx_count)
        self.end_impl(self.xml_json_file, self.xml_count)
            
    def start_impl(self, file, title):
        with open(file, "w", encoding="utf-8") as json_file:
            json_file.write("{\n")
            json_file.write("\t\"description\":\"" + title + "\",\n")
            json_file.write("\t\"title\":\"" + title + "\",\n")
            json_file.write("\t\"type\":\"array\",\n")
            json_file.write("\t\"items\": [\n")
            
    def end_impl(self, file, count):
        with open(file, "a", encoding="utf-8") as json_file:
            json_file.write("\t{\n\t\t\"count\": " + "\"" + str(count) + "\"\n\t}\n")
            json_file.write("\t]\n")
            json_file.write("}\n")
    
    def get_target_schema_field(self, name):
        if not name in self.schema_converter.keys():
            return name
        return self.schema_converter[name]
        
    def mapper(self, _, line):
        sheet = self.xlsx_source.active
        range_rows = []
        for word in WORD_RE.findall(line):
            range_rows.append(word)
        range_rows[0] = int(range_rows[0])
        range_rows[1] = int(range_rows[1])
        
        for elem in self.generate_mapper(range_rows):
            yield (range_rows[2], elem["global_id"]), elem
            
    def generate_mapper(self, range):
        if range[2] == "xlsx":
            return self.xlsx_mapper(range)
        return self.xml_mapper(range)
    
    def xlsx_mapper(self, row_range):
        sheet = self.xlsx_source.active
        elems = []
        for row in sheet.iter_rows(row_range[0], row_range[1]):
            elem = {}
            for column in range(1, sheet.max_column + 1):                elem[sheet.cell(row=1,column=column).value] = row[column-1].value
            elems.append(elem)
        return elems
    
    def xml_mapper(self, range):
        elems = []
        for child in list(self.xml_source.getroot())[range[0]: range[1] + 1]:
            elem = {}
            for childchild in list(child):
                if len(list(childchild)) == 0:
                    elem[childchild.tag] = childchild.text
                else:
                    attribs = {}
                    for attrib in list(childchild):
                        attribs[attrib.tag] = attrib.text
                    elem[childchild.tag] = attribs
            elems.append(elem)
        return elems

    def reducer(self, format_key, values):
        file = self.xlsx_json_file if format_key[0] == "xlsx" else self.xml_json_file
        name = "Bars" if format_key[0] == "xlsx" else "Coworkins"
        for value in values:
            with open(file, "a", encoding="utf-8") as json_file:
                json_file.write("\t{\n")
                json_file.write("\t\t\"" + name + "\",\n")
                json_file.write("\t\t\"description\": {\n")
                keys = list(value.keys())
                target_keys = []
                for key in keys:
                    if self.get_target_schema_field(key) in self.target_schema.keys():
                        target_keys.append(key)
                
                for key in target_keys[:-1]:
                    self.dump(json_file, key, value[key], "\",\n")
                self.dump(json_file, target_keys[-1], value[target_keys[-1]], "\"\n")
                json_file.write("\t\t}\n\t},\n")
            
    def dump(self, json_file, key, value, last):
        if isinstance(value, dict):
            temp_keys = list(value.keys())
            if self.target_schema[key]["type"] != "DICTIONARY":
                json_file.write("\t\t\t\"" + self.get_target_schema_field(key) + "\": \"" + value[temp_keys[0]] + "\"\n")
            else:
                json_file.write("\t\t\t\"" + self.get_target_schema_field(key) + "\": {\n")
                for k in temp_keys[:-1]:
                    json_file.write("\t\t\t\t\"" + self.get_target_schema_field(k) + "\": \"" + value[k] +"\",\n")
                if len(temp_keys) > 0:
                    json_file.write("\t\t\t\t\"" + self.get_target_schema_field(temp_keys[-1]) + "\": \"" + value[temp_keys[-1]] +"\"\n")
                json_file.write("\t\t\t}\n\t\t},\n")
        else:
            json_file.write("\t\t\t\"" + self.get_target_schema_field(key) + "\": \"" + value + last)
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

if __name__ == '__main__':

    converter = ConvertToJSON()
    converter.start()
    converter.run()
    converter.end()
