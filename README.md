# airflow

## Getting Started  

1. **Create a Virtual Environment**  
    A virtual environment isolates your project dependencies from the system Python installation, preventing version conflicts and ensuring a consistent development environment.
   
   Run the following command in your terminal to set up a virtual environment:  
   ```bash
   python3.9 -m venv venv
   ```

2. **Activate the Virtual Environment**  
   Use the appropriate command for your operating system:  
   - On macOS/Linux:  
     ```bash
     source venv/bin/activate
     ```  
   - On Windows:  
     ```bash
     venv\Scripts\activate
     ```

3. **Install Dependencies**  
   Install the required packages by running:  
   ```bash
   pip3 install -r requirements.txt
   ```  

## Naming Conventions
### Project Name
**quip-dw-raw-dev** is the default GCP project name when the function is to land raw data into the datawarehouse and the DAG is under development.
**quip-dw-raw** is the production version of `quip-dw-raw-dev`

### Functions
**get__**
DAGs with this prefix are exclusively used to GET data from the data source and deliver the data to our internal data warehouse.

**post__**
DAGs with this prefix are exclusively used to GET data from the data warehouse and deliver the data to an external destination.

## Best Practices

### Storing in Google Cloud Storage (GCS)
We store our json responses in GCS using the following structure:

`PROJECT_NAME/quip_airflow/FUNCTION/NAME_OF_DAG/file`

Example:

    - DAG named `get__recharge`
    - `quip-dw-raw-dev/quip_airflow_dev/get/recharge/file`
    - `quip-dw-raw/quip_airflow/get/recharge/file`
