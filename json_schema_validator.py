# Import dependencies
import json
import jsonschema
import sys
from jsonschema import validate

def json_schema_validate():
    # Defining schema
    # Modify this portion to load JSON
    # schema from a file
    schema = {
        "type" : "object",
        "properties" : {
            "price" : {"type" : "number"},
            "name" : {"type" : "string"},
        },
    }

    # Plain print of schema
    print(schema)

    # Pretty print of schema
    print(json.dumps(schema, indent=4))

    # Input data to be validated
    # Modify this portion to load JSON
    # data from a file 
    data = \
    [
        { "name": "Apples", "price": 10},
        { "name": "Bananas", "price": 20},
        { "name": "Cherries", "price": "thirty"},
        { "name": 40, "price": 40},
        { "name": 50, "price": "fifty"}
    ]

    print("Raw input data:")
    print(data)
    print("\nPretty-printed input data:")
    print(json.dumps(data, indent=4))

    print("Validating the input data using jsonschema:")
    # JSON schema validation with input data
    for idx, item in enumerate(data):
        # Try catch block to validate 
        # JSON schema with JSON data
        try:
            validate(item, schema)
            sys.stdout.write("Record #{}: OK\n".format(idx+1))
        except jsonschema.exceptions.ValidationError as ve:
            # Maintain error data file
            # Store the file info along with record info
            sys.stderr.write("Record #{}: ERROR\n".format(idx+1))
            sys.stderr.write(str(ve) + "\n")

# Call only for JSON files
json_schema_validate()