from mrjob.job import MRJob 
from mrjob.step import MRStep
from mrjob.protocol import JSONProtocol
import json
import os
import openpyxl

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
INPUT_DATA_DIR = os.path.join(SCRIPT_DIR, "data")
OUTPUT_DATA_DIR = os.path.join(SCRIPT_DIR, "out")
XLSX_FILENAME = "data-bars.xlsx"


#class JSONProtocol(object):
#
#    def read(self, line):
#        k_str, v_str = line.split('\t', 1)
#        return json.loads(k_str), json.loads(v_str)
#
#    def write(self, key, value):
#        return bytes(json.dumps({key: value}), 'utf-8')


class ConvertToJSON(MRJob):
    xlsx_source_file = openpyxl.load_workbook(os.path.join(INPUT_DATA_DIR, XLSX_FILENAME))
    # tell mrjob not to format our output -- we're going to leave that to hadooop
    OUTPUT_PROTOCOL = JSONProtocol

    # tell hadoop to massage our mrjob output using this output format
#    HADOOP_OUTPUT_FORMAT = 'nicknack.MultipleJSONOutputFormat'
    
    # mrjob 0.5.3+ only, see note below if you are using an older version
#    LIBJARS = ['nicknack-1.0.1.jar']

        
    def mapper_1(self, _, filename):
#        self.xlsx_source_file = openpyxl.load_workbook(filename)
#        sheet = self.xlsx_source_file.active
#        for column in sheet.iter_cols(1, sheet.max_column):
            yield ("hi", filename), 1

    def reducer_1(self, word_docname, counts):
        yield word_docname, sum(counts)

        
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
    ConvertToJSON().run()
