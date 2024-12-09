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
        
        # Add the inferred schema field to the list
        schema.append(bigquery.SchemaField(key, field_type))
    
    return schema

def infer_field_type(json_data, key):
    """
    Infer the BigQuery field type based on the values in the JSON data for a given key.

    Args:
        json_data (list): A list of dictionaries (JSON objects).
        key (str): The key to infer the type for.

    Returns:
        str: The corresponding BigQuery field type.
    """
    field_values = [item.get(key) for item in json_data]
    
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
        return "RECORD"  # BigQuery nested structures (repeated fields or records)
    elif all(isinstance(val, list) for val in field_values if val is not None):
        return "STRING"  # Assuming it's a list of strings for simplicity; handle lists properly if needed
    elif all(isinstance(val, (str, float, int)) for val in field_values if val is not None):
        return "STRING"  # Can be improved by more sophisticated detection for mixed types (e.g., timestamps)
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

# Print the generated schema
for field in schema:
    print(f"Field: {field.name}, Type: {field.field_type}")

# Convert SchemaField objects to a serializable format (e.g., dictionary)
schema_dict = [{"name": field.name, "type": field.field_type} for field in schema]

# Write the schema to a text file in JSON format
dataset_name = "recharge"
table_name = "credit_accounts"
with open(f"bigquery_destination_schemas/{dataset_name}/{table_name}.json", "w") as file:
    json.dump(schema_dict, file, indent=4)

