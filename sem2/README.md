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

Duplicates are considered establishments (bars and coworkings) that are sufficiently close in relative longitude and latitude (about 0.000000001).
The script `update_data.py` updates information in `task2/data`, `schema/` from `task1/out`, `schema/`.

### How to run:
```
$ cd task2
$ python update_data.py
$ python find_duplicates.py private/partition.txt
```
The resulting files in json format are located in the directory `task2/out`.
