from google.cloud import bigquery
import json

def infer_schema_from_json(json_data):
    """
    Infer BigQuery schema from a list of JSON objects.

    Args:
        json_data (list): A list of dictionaries (JSON objects).

    Returns:
        list: A list of bigquery.SchemaField objects.
    """
    schema = []
    
    # Loop through each key in the first JSON object to infer the schema
    for key in json_data[0].keys():
        # Check the data type of the value and map it to BigQuery type
        field_type = infer_field_type(json_data, key)
        
        # If the field_type is a RECORD, parse again to get the nested schema
        if field_type == "RECORD":
            sub_schema = []
            for subkey in json_data[0][key].keys():
                sub_field_type = infer_field_type(json_data, key, subkey)
                sub_field = {
                    "name": subkey,
                    "type": sub_field_type
                }
                sub_schema.append(sub_field)
            field = {
                "name": key,
                "type": field_type,
                "fields": sub_schema
            }
        else:
            field = {
                "name": key,
                "type": field_type
            }
        schema.append(field)
    return schema


def infer_field_type(json_data, key, subkey=None):
    """
    Infer the BigQuery field type based on the values in the JSON data for a given key.

    Args:
        json_data (list): A list of dictionaries (JSON objects).
        key (str): The key to infer the type for.

    Returns:
        string: The corresponding BigQuery field type 
    """
    if subkey:
        field_values = [item.get(key).get(subkey) for item in json_data]
    else:
        field_values = [item.get(key) for item in json_data]
    # If all values are None or empty, default to STRING type
    if not any(val is not None for val in field_values):
        return "STRING"
    # Infer the field type based on the types of the values in the JSON data
    if all(isinstance(val, int) for val in field_values if val is not None):
        return "INTEGER"
    elif all(isinstance(val, float) for val in field_values if val is not None):
        return "FLOAT"
    elif all(isinstance(val, str) for val in field_values if val is not None):
        return "STRING"
    elif all(isinstance(val, bool) for val in field_values if val is not None):
        return "BOOLEAN"
    elif all(isinstance(val, dict) for val in field_values if val is not None):
        # If it's a dictionary, treat it as a RECORD type and infer nested schema
        return "RECORD"  # Return the nested schema for RECORD
    elif all(isinstance(val, list) for val in field_values if val is not None):
        print(f"Warning: List type detected for key '{key}'. Inferring as STRING...Manually check if this needs to be updated to mode: REPEATED")
        return "STRING"
    elif all(isinstance(val, (str, float, int)) for val in field_values if val is not None):
        return "STRING"  # Mixed types default to STRING
    else:
        return "STRING"  # Default type if we can't infer it




# MANUALLY RUN HERE
# Set the JSON data to generate the schema from
json_data = [ # Example JSON data
    {
		"id": 1,
		"name": "Alice",
		"age": 30,
		"is_student": False
	},
	{
		"id": 2,
		"name": "Bob",
		"age": 25,
		"is_student": True
	}
]

# Generate BigQuery schema from the JSON data
schema = infer_schema_from_json(json_data)

# Write the schema to a text file in JSON format
dataset_name = "recharge"
table_name = "events"
with open(f"bigquery_destination_schemas/{dataset_name}/{table_name}.json", "w") as file:
    json.dump(schema, file, indent=4)

