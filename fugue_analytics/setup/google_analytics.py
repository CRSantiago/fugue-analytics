from fugue_analytics.metrics.sinks.postgres import insert_df_to_table
import pandas as pd
from apiclient.discovery import build 
from oauth2client.service_account import ServiceAccountCredentials

import os

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

df = pd.DataFrame()

# Parse the data and save it do a dataframe
for report in response.get('reports', []):

    columnHeader = report.get('columnHeader', {})
    dimensionHeader = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
   
    rows = report.get('data', {}).get('rows', [])

    df_columns = dimensionHeader + [head['name'] for head in metricHeaders]
    df_rows = []
        
    for row in rows:
        dimension = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])[0].get('values', [])
            
        df_row = dimension + [int(value) for value in dateRangeValues]
        df_rows.append(df_row)
    
    df[f"{df_columns[0]}"] = pd.Series(df_rows)
    