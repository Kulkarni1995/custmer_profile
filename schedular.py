from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from datetime import datetime

####################################
## variable declaration
RAW_LAYER_PY = 'gs://your-bucket-name/raw_layer.py'  # Cloud Storage path or local path
CUST_PROFILE_PY = 'gs://your-bucket-name/cust_profile.py'  # Cloud Storage path or local path
CUST_TRANSITION_PY = 'gs://your-bucket-name/cust_transition.py'  # Cloud Storage path or local path
GOLD_LAYER_PY = 'gs://your-bucket-name/gold_layer.py'  # Cloud Storage path or local path


GOOGLE_APPLICATION_CREDENTIALS = r'C:\Users\HP\Desktop\data_projects\evaluation\service_key\delta-compass-440906-1551790f643b.json'
PROJECT_NAME = 'delta-compass-440906'
BUCKET_NAME = 'project-file-raw-layerfiles'
JOB_NAME_DAILY = f'daily{DATE_STR}'
LOCATION = 'us-central1'
STAGING = f'gs://{BUCKET_NAME}/staging'
TEMP = f'gs://{BUCKET_NAME}/temp'
####################################

## common udf
def run_python_script(path):
    try:
        # Execute the Python file from the given path
        exec(open(path).read())
    except Exception as e:
        print(f"Error executing script at {path}: {e}")

# Define the DAG
dag = DAG(
    'datapipeline_file_in_composer',
    description='Run Python script in Cloud Composer',
    schedule_interval='@daily',  # Or any other schedule
    start_date=datetime(2025, 1, 20),
    catchup=False,
)

# Define the task using PythonOperator
run_task1 = PythonOperator(
    task_id='raw_layer',
    python_callable=run_python_script,
    op_args=[RAW_LAYER_PY],  # Pass the path to the script as an argument
    dag=dag,
)

run_task2 = DataflowCreatePythonJobOperator(
        task_id='run_dataflow_pipeline_daily_silver_cust_profile',
        job_name='beam-dataflow-job',  # Unique job name
        py_file=CUST_PROFILE_PY ,  # Path to the pipeline on GCS
        location=LOCATION,
        gcp_conn_id='earth_conn',
        options={
            'project': PROJECT_NAME,
            'region': LOCATION,
            'temp_location': f'gs://{BUCKET_NAME}/temp',
            'staging_location': f'gs://{BUCKET_NAME}/staging',  # Fixed typo: "stagging" → "staging"
            'runner': 'DataflowRunner',
            'input': f"gs://{BUCKET_NAME}/input/",
            'output': f"gs://{BUCKET_NAME}/output/",
        })

run_task3 = run_task2 = DataflowCreatePythonJobOperator(
        task_id='run_dataflow_pipeline_daily_silver_cust_transition',
        job_name='beam-dataflow-job',  # Unique job name
        py_file=CUST_TRANSITION_PY ,  # Path to the pipeline on GCS
        location=LOCATION,
        gcp_conn_id='earth_conn',
        options={
            'project': PROJECT_NAME,
            'region': LOCATION,
            'temp_location': f'gs://{BUCKET_NAME}/temp',
            'staging_location': f'gs://{BUCKET_NAME}/staging',  # Fixed typo: "stagging" → "staging"
            'runner': 'DataflowRunner',
            'input': f"gs://{BUCKET_NAME}/input/",
            'output': f"gs://{BUCKET_NAME}/output/",
        })

run_task4 = DataflowCreatePythonJobOperator(
        task_id='run_dataflow_pipeline_daily_gold',
        job_name='beam-dataflow-job',  # Unique job name
        py_file=GOLD_LAYER_PY ,  # Path to the pipeline on GCS
        location=LOCATION,
        gcp_conn_id='earth_conn',
        options={
            'project': PROJECT_NAME,
            'region': LOCATION,
            'temp_location': f'gs://{BUCKET_NAME}/temp',
            'staging_location': f'gs://{BUCKET_NAME}/staging',  # Fixed typo: "stagging" → "staging"
            'runner': 'DataflowRunner',
            'input': f"gs://{BUCKET_NAME}/input/",
            'output': f"gs://{BUCKET_NAME}/output/",
        })

# Set task dependencies
run_task1 >> [run_task2, run_task3] >> run_task4