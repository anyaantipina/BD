# BD

## Task 0

<strong>Searching for structured data in different formats</strong>

The data is located in the directory `task1/data`.
I chose two data sources:
1) Archive of Moscow bars in xlsx format: https://data.mos.ru/opendata/7710881420-bary
2) Archive of Moscow coworkings in xml format: https://data.gov.ru/opendata/7710071979-coworking
   
## Task 1

<strong>Loading heterogeneous data resources</strong>


### How to run:
```
$ cd task1
$ python convert_to_json.py private/partition.txt
```
The resulting files in json format are located in the directory `task1/out`.

## Task 2

<strong>Finding duplicates</strong>

To search for duplicates the similarity of elements is calculated taking into account the weight vector. The similarity criteria are: longitude and latitude (weight 0.8), string proximity of addresses with missing values (weight 0.15), matching phone numbers (weight 0.05).  

The script `update_data.py` updates information in `task2/data`, `schema/` from `task1/out`, `schema/`.

### How to run:
```
$ cd task2
$ python update_data.py
$ python find_duplicates.py private/partition.txt
```
The resulting files in json format are located in the directory `task2/out`.

## Task 3

<strong>Data Fusion</strong>

Taking into account the criteria of similarity of elements, in each set of duplicates there are establishments that have the same or close address. 
Conflicts are resolved according to the following scheme: 
*  Information about the class, name, and phone number is saved for each object and placed in the list of establishments at this address. 
*  The address is extracted, parsed by components, each of which is compared by string proximity with the others. If there are different addresses, then a set of the most relevant objects is compiled (those with fewer empty fields). Next, a concatenation of addresses is made from this set (I configured the parameters so that only two different buildings/buildings are possible).  

The script `update_data.py` updates information in `task3/data` from `task1/out`, `task2/out`.

### How to run:
```
$ cd task3
$ python update_data.py
$ python data_fusion.py private/partition.txt
```
The resulting files in json format are located in the directory `task3/out`.
