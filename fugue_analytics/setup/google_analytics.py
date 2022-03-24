from fugue_analytics.metrics.sinks.postgres import insert_df_to_table
import pandas as pd
from apiclient.discovery import build 
from oauth2client.service_account import ServiceAccountCredentials

import os

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'google_api_key.json'
VIEW_ID = os.environ['FUGUE-DOCS-VIEWID']

credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)

analytics = build('analyticsreporting', 'v4', credentials=credentials)

response = analytics.reports().batchGet( 
    body={ 
        'reportRequests': [ 
                { 
                    'viewId': VIEW_ID, 
                    'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'today'}], 
                    'metrics': [{'expression': 'ga:pageviews'}], 
                    "dimensions": [{'name': 'ga:country'}, {'name': 'ga:sourceMedium'}]
                },
            ] 
        }).execute()

print(response)