# airflow

## Getting Started  

**Clone Directory for Composer Local Development**
git clone https://github.com/GoogleCloudPlatform/composer-local-dev.git

**Clone the airflow Repository into this directory
cd composer-local-dev
git clone https://github.com/getquip/airflow.git

**Create a Virtual Environment**  
    A virtual environment isolates your project dependencies from the system Python installation, preventing version conflicts and ensuring a consistent development environment.
   
   Run the following command in your terminal to set up a virtual environment:  
   ```bash
   python3 -m venv venv
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
   pip3 install -r airflow/requirements.txt
   pip3 install .
   ```  

composer-dev create airflow-local \
    --from-source-environment  composer-dev \
    --location us-central1 \
    --project quip-dw-raw-dev \
    --port 8081 \
    --dags-path airflow/dags

    Troubleshooting: Ensure your docker version is up-to-date. try running `pip3 install docker --upgrade`

# these are inherently installed in composer's airflow
apache-airflow==2.10.2 # For local development and testing
pip install apache-airflow-providers-google



## Naming Conventions

**get__**
DAGs with this prefix are exclusively used to GET data from the data source and deliver the data to our internal data warehouse.

**post__**
DAGs with this prefix are exclusively used to GET data from the data warehouse and deliver the data to an external destination.

## Best Practices

### Storing in Google Cloud Storage (GCS)
We store our json responses in GCS using the following structure:

`PROJECT_NAME/airflow/FUNCTION/NAME_OF_DAG/file`

Example:
    - DAG named `get__recharge`
    - `quip-dw-raw-dev/quip_airflow/get/recharge/file`
    - `quip-dw-raw/quip_airflow/get/recharge/file`