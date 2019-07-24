# JSON to CSV converter
# This step will follow
# after successful JSON schema 
# validation
import csv
import json

employee_data = '{"employee_details":[{"employee_name": "James", "email": "james@gmail.com", "job_profile": ["Sr. Developer","Quick Response Member"]},{"employee_name": "Smith", "email": "Smith@gmail.com", "job_profile": ["Project Lead","Influencer"]}]}'

# Parse JSON data using JSON loads
emp_parsed = json.loads(employee_data)

# Pretty print JSON using JSON dumps and 
# indent
print(json.dumps(emp_parsed,indent=4))

# Grab employee details
# using parent object
emp_data = emp_parsed['employee_details']

count = 0
for emp in emp_data:
    if count == 0:
        header = emp.keys()
        print(header)
        count += 1
    print(emp.values())
    
# https://stackoverflow.com/questions/1871524/how-can-i-convert-json-to-csv

'''
def flattenjson( b, delim ):
    val = {}
    for i in b.keys():
        if isinstance( b[i], dict ):
            get = flattenjson( b[i], delim )
            for j in get.keys():
                val[ i + delim + j ] = get[j]
        else:
            val[i] = b[i]

    return val
'''



