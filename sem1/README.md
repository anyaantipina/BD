# BD

## Task 1

<strong>The implementation of the algorithm <em>TF*IDF</em></strong>

* payload consists 3 files and places in the directory `data`

### How to run:
* type
```
$ cd task1
$ python tfidf.py data/
Enter a template: 
```
* input template string with words for search, for example, "doc def import"
* output in terminal means output for the most relevant documents from the payload directory
```
"file:\/\/dir\/file1.py"	0.0221162786
"file:\/\/dir\/file2.py"	0.0117715676
"file:\/\/dir\/.DS_Store"	0
```
* relevance metric is calculated as the arithmetic mean of the <em>tf*idf</em> values of words from the template string
   
## Task 2

<strong>Text extraction about the <em>Transportation</em> section of each country from payload file</strong>

* payload file consists information about 10 countries

### How to run:
* install <strong>JDK 7</strong> and <strong>Eclipse 4.2.*</strong> if not done before
* download and install <strong>IBM BigInsights</strong>
* run from <em>BigInsights Project</em> in Eclipse
