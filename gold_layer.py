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

## bq configuration

DATASET_NAME = 'evaluation'
TABLE_NAME_PROFILE = 'cust_profile'
TABLE_NAME_TRANS = 'cust_tran'
TABLE_NAME_PROFILE = 'cust_profile'
TABLE_TRANS =  f'{PROJECT_NAME}.{DATASET_NAME}.{TABLE_NAME_TRANS}'
TABLE_PROFILE = f'{PROJECT_NAME}.{DATASET_NAME}.{TABLE_NAME_PROFILE}'

CUST_TOTAL_ORDER = f'{PROJECT_NAME}.{DATASET_NAME}.cust_total_order'
CAT_TOTAL_ORDER = f'{PROJECT_NAME}.{DATASET_NAME}.catagory_total_order'
CUST_PROFILE = f'{PROJECT_NAME}.{DATASET_NAME}.customer_profile_with_spending'

## udf
## select required colm
class filter_col(beam.DoFn):
    def process(self, element,col1,col2):
        output = (element[col1],element[col2])
        yield output

## finding total amount
class sum_total(beam.DoFn):
    def process(self, element,col1):
        total = round(sum(element[1]),2)
        yield {col1:element[0],'total_amount':total}
## analysis part

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
        read_from_trans = (
                p
                | 'Read from BigQuery' >> beam.io.gcp.bigquery.ReadFromBigQuery(table=TABLE_TRANS)
        )

        # Select specific columns (customer_id and amount)
        cust_total_order = (
                # read_from_trans
                # | 'Select particular columns' >> beam.ParDo(filter_col(), col1='customer_id', col2='amount')
                # | 'group by col' >>  beam.GroupByKey()
                # | 'sum per cust' >> beam.ParDo(sum_total(),col1='customer_id')
                # | 'Write Transactions to BigQuery' >> beam.io.WriteToBigQuery(
                #     table=CUST_TOTAL_ORDER,
                #     schema='SCHEMA_AUTODETECT',
                #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                #     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                #
                # )
                #  | 'Print' >> beam.Map(print)  # To print the results
        )

        # category_wise_total = (
        #         read_from_trans
        #         | 'Select particular columns' >> beam.ParDo(filter_col(), col1='category', col2='amount')
        #         | 'group by col' >>  beam.GroupByKey()
        #         | 'sum per cust' >> beam.ParDo(sum_total(),col1='category')
        #         | 'Write Transactions to BigQuery' >> beam.io.WriteToBigQuery(
        #             table=CAT_TOTAL_ORDER,
        #             schema='SCHEMA_AUTODETECT',
        #             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        #             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        #         )
        #         | 'Print' >> beam.Map(print)  # To print the results
        # )

        with beam.Pipeline(options=options) as p1:
            query = """
                            SELECT
                          cp.customer_id,
                          cp.first_name,
                          cp.last_name,
                          cp.email,
                          cp.signup_date,
                          cp.year,
                          cp.month,
                          cp.day,
                          ROUND(SUM(ct.amount),2) AS total_spending
                        FROM
                          delta-compass-440906.evaluation.cust_profile cp
                        LEFT JOIN
                          delta-compass-440906.evaluation.cust_tran ct
                        ON
                          cp.customer_id = ct.customer_id
                        GROUP BY
                          cp.customer_id,
                          cp.first_name,
                          cp.last_name,
                          cp.email,
                          cp.signup_date,
                          cp.year,
                          cp.month,
                          cp.day
                        ORDER BY
                          total_spending DESC;
                    """
            data_from_bq = (
                    p | 'read' >> beam.io.gcp.bigquery.ReadFromBigQuery(query=query, use_standard_sql=True)
                #     | 'Write Transactions to BigQuery' >> beam.io.WriteToBigQuery(
                #     table=CUST_PROFILE,
                #     schema='SCHEMA_AUTODETECT',
                #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                #     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                # )
                    | 'Print' >> beam.Map(print)
            )