import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from datetime import datetime
import json
import os
from datetime import datetime
import re  # Added re for regex operations

### global variable define
DATE_STR = datetime.now().strftime('%Y%m%d')
GOOGLE_APPLICATION_CREDENTIALS = r'C:\Users\HP\Desktop\data_projects\evaluation\service_key\delta-compass-440906-1551790f643b.json'
PROJECT_NAME = 'delta-compass-440906'
BUCKET_NAME = 'project-file-raw-layerfiles'
JOB_NAME_DAILY = f'daily{DATE_STR}'
LOCATION = 'us-central1'
STAGING = f'gs://{BUCKET_NAME}/staging'
TEMP = f'gs://{BUCKET_NAME}/temp'

## raw_dataset
CUST_TRANS ='gs://project-file-raw-layerfiles/cust_tran/customer_transcation.csv'

## bq variable
DATASET_NAME = 'evaluation'
TABLE_NAME_PROFILE = 'cust_profile'
TABLE_NAME_TRANS = 'cust_tran'
TABLE_TRANS =  f'{PROJECT_NAME}.{DATASET_NAME}.{TABLE_NAME_TRANS}'

### functions define

# splitting row
class SplitRow(beam.DoFn):
    def process(self, element):
        return [element.split(',')]

def remove_duplicate(element):
    obj = element[1]

        # If there are multiple entries
    if len(obj) > 1:
            # Extract the dates (assuming they are at index 3 of each entry)
        dates = [entry[3] for entry in obj]

            # Find the maximum date
        max_date = max(dates)

            # Find the entry with the maximum date
        for i in obj:
            if i[3] == max_date:  # Check if the max date matches the date in the entry
                output = (element[0], i)
                return output

        # If there's only one entry or no duplicates
    else:
        output = (element[0],element[1][0])

        return output

## transformations
def parse_customer_transaction(record):
    """
    Parses customer transaction data into a structured dictionary with enrichment.
    """
    record[1][3] = str(record[1][3]).strip()
    date = datetime.strptime(record[1][1], '%Y-%m-%d %H:%M:%S')

    if float(record[1][2]) <= 1000000 and float(record[1][2]) >= 0:
        return {
            'transaction_id': record[0],
            'customer_id': record[1][0],
            'transaction_date': record[1][1],
            'amount': float(record[1][2]),
            'category': re.sub(r'[^a-zA-Z0-9\s]', '', record[1][3]),
            'is_large_transaction': float(record[1][2]) > 1000,
            'year': date.year,
            'month': date.month,
            'day': date.day
        }






if __name__ == "__main__":

    # Set pipeline options
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_NAME
    google_cloud_options.job_name = JOB_NAME_DAILY
    google_cloud_options.region = LOCATION
    google_cloud_options.staging_location = STAGING
    google_cloud_options.temp_location = TEMP





    with beam.Pipeline(options=options) as p:

            # Read and process customer transactions
        customer_transactions = (
                p
                | 'Read Customer Transactions' >> beam.io.ReadFromText(CUST_TRANS, skip_header_lines=1)
                | 'Split Customer Transactions' >> beam.ParDo(SplitRow())
                |  'creating_tuple' >> beam.Map(lambda word: (word[0], word[1:]))
                |'Group produce per season' >> beam.GroupByKey()
                | 'remove duplicate' >> beam.Map(remove_duplicate)
                 | 'Parse Customer Transactions' >> beam.Map(parse_customer_transaction)
                | 'Write Transactions to BigQuery' >> beam.io.WriteToBigQuery(
                    table=TABLE_TRANS,
                    schema='SCHEMA_AUTODETECT',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,

                )
                #   | 'print' >> beam.Map(print)
            )



# def get_latest_transaction(records):
#     """
#     Get the latest transaction based on the transaction_date.
#     """
#     records.sort(key=lambda x: datetime.strptime(x[2], '%Y-%m-%d %H:%M:%S'), reverse=True)
#     return records[0]

    # with beam.Pipeline(options=options) as p:
    #     try:
    #         # Read and process customer transactions
    #         customer_transactions = (
    #             p
    #             | 'Read Customer Transactions' >> beam.io.ReadFromText(CUST_TRANS, skip_header_lines=1)
    #             | 'Split Customer Transactions' >> beam.ParDo(SplitRow())
    #             # | 'Key Transactions by Customer ID' >> beam.Map(lambda x: (x[1], x))  # Key by customer_id
    #             # | 'Group Transactions by Customer ID' >> beam.GroupByKey()
    #             # | 'Get Latest Transaction' >> beam.Map(lambda x: get_latest_transaction(list(x[1])))
    #             | 'Parse Customer Transactions' >> beam.Map(parse_customer_transaction)
    #             | 'Write Transactions to BigQuery' >> beam.io.WriteToBigQuery(
    #                 table=TABLE_TRANS,
    #                 schema='SCHEMA_AUTODETECT',
    #                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    #
    #             )
    #              # | 'print' >> beam.Map(print)
    #         )