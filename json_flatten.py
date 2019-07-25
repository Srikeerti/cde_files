import json
import csv
from collections import OrderedDict

with open('C:/Users/koakhila/Documents/sample.json','r') as f:
    data = json.load(f,object_pairs_hook=OrderedDict)
    #data = json.load(f)
    print(data)
ovia = data['report']['records']
count = 0
for i in ovia:
    if count ==0:
        print("Going to write CSV Header")
        #print(type(i))
        #print(list(i.keys()))
        header = list(i.keys())
        with open('C:/Users/koakhila/Desktop/file.csv', 'w') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            wr.writerow(header)
        count+=1
    #else:
    print("Going to write CSV data")
    #print(list(i.values()))
    values = list(i.values())
    with open('C:/Users/koakhila/Desktop/file.csv', 'a') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            wr.writerow(values)