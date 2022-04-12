from fugue_analytics.metrics.sinks.postgres import insert_df_to_table
import pandas as pd
from apiclient.discovery import build 
from oauth2client.service_account import ServiceAccountCredentials

import datetime

import os

def initialize_ga_stats():
    # Load in GA credentials from the dotenv file
    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
    KEY_FILE_LOCATION = 'google_api_key.json'
    VIEW_ID = os.environ['FUGUE-DOCS-VIEWID']

    # Set up GA connection
    credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    # Query GA API
    response = analytics.reports().batchGet( 
        body={ 
            'reportRequests': [ 
                    { 
                        'viewId': VIEW_ID, 
                        'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'today'}], 
                        "dimensions": [{'name': 'ga:country'}]
                    },
                    { 
                        'viewId': VIEW_ID, 
                        'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'today'}], 
                        'metrics': [{'expression': 'ga:pageviews'}], 
                    },
                    { 
                        'viewId': VIEW_ID, 
                        'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'today'}], 
                        "dimensions": [{'name': 'ga:sourceMedium'}]
                    },
                ] 
            }).execute()

    columns_names = ['datetime', 'source', 'value']
    df = pd.DataFrame(columns=columns_names)

    # Parse the data and save it do a dataframe
    for report in response.get('reports', []):
        col = 0
        columnHeader = report.get('columnHeader', {})
        dimensionHeader = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    
        rows = report.get('data', {}).get('rows', [])

        df_columns = dimensionHeader + [head['name'] for head in metricHeaders]
        
        if(len(df_columns) == 2):
            metric_s = f"{df_columns[0]} - {df_columns[1]}"
        else:
            metric_s = f"{df_columns[0]}"
        
        df_rows = []
        for row in rows:
            metric_e = metric_s
            dimension = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])[0].get('values', [])

            date = datetime.date.today()
                
            df_row = dimension + [int(value) for value in dateRangeValues]
            if(len(df_row) == 2):
                metric_e += f" - {df_row[0]}"
                value = df_row[1]
            else:
                value = df_row[0]
            df.loc[len(df)] = [date, metric_e, value]

    insert_df_to_table(df, "metrics_over_time")

if __name__ == "__main__":
    initialize_ga_stats()


        
        
        

