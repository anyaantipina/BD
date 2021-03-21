from mrjob.job import MRJob 
from mrjob.step import MRStep 
import re
import os
import math

WORD_RE = re.compile(r"[\w']+")

template = ""

class MRMostUsedWord(MRJob):
    def mapper_1(self, _, line):
        fname = os.environ['map_input_file']
        for word in WORD_RE.findall(line):
            yield (word.lower(), fname), 1

    def reducer_1(self, word_docname, counts):
        yield word_docname, sum(counts)
        
    def mapper_2(self, word_docname, counts):
        yield word_docname[1], (word_docname[0], counts)
    
    def reducer_2(self, docname, word_counts):
        N = 0;
        new_word_counts = []
        for word, counts in word_counts:
            N += counts
            new_word_counts.append((word, counts))
        for word, counts in new_word_counts:
            yield (word, docname), (counts, N)
    
    def mapper_3(self, word_docname, counts_N):
        yield word_docname[0], (word_docname[1], counts_N[0], counts_N[1], 1)
    
    def reducer_3(self, word, docname_counts_N_1):
        new_docname_counts_N_1 = []
        m = 0
        for docname, counts, N, one in docname_counts_N_1:
            m+=1
            new_docname_counts_N_1.append((docname, counts, N, one))
        for docname, counts, N, one in new_docname_counts_N_1:
            yield None, (word, docname, counts, N, m)
            
    def reducer_4(self, _, word_docname_counts_N_m):
        D = 0
        docs = set()
        new_word_docname_counts_N_m = []
        for a1, docname, a2, a3, a4 in word_docname_counts_N_m:
            docs.add(docname)
            new_word_docname_counts_N_m.append((a1, docname, a2, a3, a4))
        D = len(docs)
        for word, docname, counts, N, m in new_word_docname_counts_N_m:
            yield (word, docname), (counts, N, m, D)
            
    def mapper_5(self, word_docname, counts_N_m_D):
        tfidf = (counts_N_m_D[0] / counts_N_m_D[1]) * math.log(counts_N_m_D[3] / counts_N_m_D[2])
        yield word_docname[1], (word_docname[0], tfidf)
    
    def reducer_5(self, docname, word_tfidf):
        search_word = WORD_RE.findall(template)
        num_search_words_in_doc = 0
        sum_tfidf_search_words_in_doc = 0
        for word, tfidf in word_tfidf:
            if word in search_word:
                num_search_words_in_doc += 1
                sum_tfidf_search_words_in_doc += tfidf
            
        average_tfidf = (sum_tfidf_search_words_in_doc / num_search_words_in_doc) if (num_search_words_in_doc != 0) else 0
        yield None, (average_tfidf, docname)
        
    def reducer_6(self, _, average_tfidf_docname):
        for tfidf, doc in sorted(average_tfidf_docname, reverse=True):
            yield doc, tfidf
        
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_1,
                   reducer=self.reducer_1),
            MRStep(mapper=self.mapper_2,
                   reducer=self.reducer_2),
            MRStep(mapper=self.mapper_3,
                   reducer=self.reducer_3),
            MRStep(reducer=self.reducer_4),
            MRStep(mapper=self.mapper_5,
                   reducer=self.reducer_5),
            MRStep(reducer=self.reducer_6)
        ]

if __name__ == '__main__':
    template += input("Enter a template: ")
    print(template)
    MRMostUsedWord.run()
