import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from datetime import datetime
import json
import os
from datetime import datetime
import re  # Added re for regex operations


DATE_STR = datetime.now().strftime('%Y%m%d')


GOOGLE_APPLICATION_CREDENTIALS = r'C:\Users\HP\Desktop\data_projects\evaluation\service_key\delta-compass-440906-1551790f643b.json'
PROJECT_NAME = 'delta-compass-440906'
BUCKET_NAME = 'project-file-raw-layerfiles'
JOB_NAME_DAILY = f'daily{DATE_STR}'
LOCATION = 'us-central1'
STAGING = f'gs://{BUCKET_NAME}/staging'
TEMP = f'gs://{BUCKET_NAME}/temp'

CUST_PROFILE = 'gs://project-file-raw-layerfiles/cust_profile/customer_profile.csv'
CUST_TRANS ='gs://project-file-raw-layerfiles/cust_tran/customer_transcation.csv'



DATASET_NAME = 'evaluation'
TABLE_NAME_PROFILE = 'cust_profile'
TABLE_PROFILE = f'{PROJECT_NAME}.{DATASET_NAME}.{TABLE_NAME_PROFILE}'

TABLE_NAME_TRANS = 'cust_tran'

TABLE_TRANS =  f'{PROJECT_NAME}.{DATASET_NAME}.{TABLE_NAME_TRANS}'

if __name__ == "__main__":

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_NAME
    google_cloud_options.job_name = JOB_NAME_DAILY
    google_cloud_options.region = LOCATION
    google_cloud_options.staging_location = STAGING
    google_cloud_options.temp_location = TEMP


## function for cust_profile_transformation

    ## spliting csv
    def convert_to_list(element):

        data = element.split(',')# split on comma
        return data
    ## removing duplicate



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


    def create_dict(data):

        my_dict = {'customer_id':data[0],'first_name':data[1][0],'last_name':data[1][1],'email':data[1][2],'signup_date':data[1][3]}

        return my_dict


    class date_part(beam.DoFn):
        def process(self, element):
            date = element.get('signup_date', '2099-12-01')
            email_varification = element.get('email', 'null')
            data = date.split('-')
            if len(data) == 3 and email_varification != 'null':
                element['year'] = data[0]
                element['month'] = data[1]
                element['day'] = data[2]

                yield element
    ## for 2nd data frame





    # ## cust _profile_reading
    with beam.Pipeline(options=options) as p:
        cust_prof_raw = (p
                            | 'Read Raw Data' >> beam.io.ReadFromText(CUST_PROFILE,skip_header_lines=1)
                            | 'split data' >> beam.Map(convert_to_list)  ## with new date year month and day col
                            | 'creating_tuple' >> beam.Map(lambda word: (word[0], word[1:]))
                            |'Group produce per season' >> beam.GroupByKey()
                            | 'remove duplicate' >> beam.Map(remove_duplicate)
                            | 'convert to dict' >> beam.Map(create_dict)
                            | 'date part' >> beam.ParDo(date_part())
                            # | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                            #  table=TABLE_PROFILE,
                            # schema='SCHEMA_AUTODETECT',
                            # write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                            # create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
                              | 'print' >> beam.Map(print)

                    )



