with open('sample.json','r') as f:
    data = json.load(f)
ovia = data['report']['records']
count = 0
for i in ovia:
    if count ==0:
        print("Going to write CSV Header")
        #print(type(i))
        #print(list(i.keys()))
        header = list(i.keys())
        with open('file.csv', 'w', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            wr.writerow(header)
        count+=1
    #else:
    print("Going to write CSV data")
    #print(list(i.values()))
    values = list(i.values())
    with open('file.csv', 'a', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            wr.writerow(values)