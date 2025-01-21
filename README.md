# Airflow Project Setup

## Overview
This repository contains the necessary setup for working with Airflow on Google Cloud Composer. It is used for developing, testing, and deploying Airflow DAGs to our Composer environment.

## Composer Environments
There are two Composer environments:
- **Development**: Used for testing and development purposes.
- **Production**: Used for production workflows and running DAGs in the live environment.

GitHub Actions workflows only deploy DAGs to the production environment.

## Airflow DAGs
Within the Airflow environment, all DAGs are executed from the `dags/` directory. This directory is pushed directly to the Composer GCS bucket, and the DAGs in it are scheduled and run according to the configuration in the Composer environment.

## Directory Structure

- **`dags/`**  
  This is the primary directory where all Airflow DAGs are stored. It is pushed directly to the Composer GCS bucket for execution in the cloud environment.

- **`plugins/developer_helpers/`**  
  This directory contains Python scripts and tools that are helpful for developers during development. These scripts are not intended to be used in Airflow DAGs but can assist in local testing, debugging, and utility functions.

## Getting Started

1. **Clone the repository**  
   Clone the repository to your local development environment:
   ```bash
   git clone https://github.com/getquip/airflow.git
   cd airflow
   ```

2. **Set up a virtual environment**  
   It is recommended to set up a virtual environment to isolate your project dependencies:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On macOS/Linux
   ```

3. **Install dependencies**  
   Install the required packages:
   ```bash
   pip3 install -r requirements.txt
   ```

4. **Pushing DAGs to Google Cloud Composer**  
   Push the DAGs stored in the `dags/` directory to the Composer GCS bucket. Ensure your DAG files are correctly uploaded for execution in the cloud environment.

## Naming Conventions

- **`get__`**  
  DAGs with this prefix are used to **GET** data from a data source and deliver it to our internal data warehouse.

- **`post__`**  
  DAGs with this prefix are used to **POST** data from the data warehouse to an external destination.

## Best Practices

### Storing in Google Cloud Storage (GCS)
We store JSON responses in GCS using the following structure:

`PROJECT_NAME/airflow/FUNCTION/NAME_OF_DAG/file_path`

Example:
  - DAG named `get__recharge`
  - Stored at: `quip-dw-raw-dev/quip_airflow/get/recharge/events/file.json`
  - GCS path: `quip-dw-raw/quip_airflow/get/recharge/events/file.json`
 