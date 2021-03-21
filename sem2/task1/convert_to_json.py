from mrjob.job import MRJob 
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol
import json
import os
import openpyxl
import re
from math import ceil

WORD_RE = re.compile(r"[\w']+")
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
INPUT_DATA_DIR = os.path.join(SCRIPT_DIR, "data")
INTERNAL_DIR = os.path.join(SCRIPT_DIR, "private")
OUTPUT_DATA_DIR = os.path.join(SCRIPT_DIR, "out")
XLSX_FILENAME = "data-bars.xlsx"
INTERNAL_FILE = "internal_xlsx.txt"


#class JSONProtocol(object):
#
#    def read(self, line):
#        k_str, v_str = line.split('\t', 1)
#        return json.loads(k_str), json.loads(v_str)
#
#    def write(self, key, value):
#        return bytes(json.dumps({key: value}), 'utf-8')

class Splitter():
    def run(self):
        xlsx_source_file = openpyxl.load_workbook(os.path.join(INPUT_DATA_DIR, XLSX_FILENAME))
        sheet = xlsx_source_file.active
        partition_file = open(os.path.join(INTERNAL_DIR, "partition.txt"), "w")
        per_mapper = ceil(sheet.max_row / 10)
        i = 2
        max_row = sheet.max_row
        while i < max_row:
            start = i
            end = i + per_mapper if (i + per_mapper <= sheet.max_row) else sheet.max_row
            partition_file.write("%s %s" % (start,end))
            i = end
            if i < max_row:
                partition_file.write("\n")
        partition_file.close()

class ConvertToJSON(MRJob):
    xlsx_source_file = openpyxl.load_workbook(os.path.join(INPUT_DATA_DIR, XLSX_FILENAME))
#    internal_representation_file = open(os.path.join(INTERNAL_DIR, INTERNAL_FILE), "w")
    structure = []
    # tell mrjob not to format our output -- we're going to leave that to hadooop
#    OUTPUT_PROTOCOL = RawProtocol

    # tell hadoop to massage our mrjob output using this output format
#    HADOOP_OUTPUT_FORMAT = 'nicknack.MultipleJSONOutputFormat'
    
    # mrjob 0.5.3+ only, see note below if you are using an older version
#    LIBJARS = ['nicknack-1.0.1.jar']
#    def init(self):
#        sheet = self.xlsx_source_file.active
#        for column in sheet.iter_cols(1, sheet.max_column):
#            self.structure.append(column[0].value)
#        for row in sheet.iter_rows(2, sheet.max_row):
#            for field in row:
#                self.internal_representation_file.write("%s;" % field.value)
#            self.internal_representation_file.write("\n")
#        self.internal_representation_file.close()
        
    def mapper_1(self, _, line):
        sheet = self.xlsx_source_file.active
        fname = os.environ['map_input_file']
        range = []
        for word in WORD_RE.findall(line):
            range.append(int(word))
        for row in sheet.iter_rows(range[0], range[1]):
            elem = []
            key = row[0].value
            for field in row:
                elem.append(field.internal_value.encode('utf8'))
            yield (fname, key), elem

    def reducer_1(self, key, value):
        yield key, value

        
#    def reducer_6(self, _, average_tfidf_docname):
#        for tfidf, doc in sorted(average_tfidf_docname, reverse=True):
#            yield json.dumps(["dirname1", "key1"]), json.dumps({"name": doc})
#            yield json.dumps(["dirname2", "key2"]), json.dumps({"tfidf": tfidf})
        
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_1,
                   reducer=self.reducer_1)
#            MRStep(mapper=self.mapper_2,
#                   reducer=self.reducer_2),
#            MRStep(mapper=self.mapper_3,
#                   reducer=self.reducer_3),
#            MRStep(reducer=self.reducer_4),
#            MRStep(mapper=self.mapper_5,
#                   reducer=self.reducer_5),
#            MRStep(reducer=self.reducer_6)
        ]

if __name__ == '__main__':
    Splitter().run()
    converter = ConvertToJSON()
#    converter.init()
    converter.run()
