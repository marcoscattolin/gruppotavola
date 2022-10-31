"""Based on Google Analytics Reporting API V4."""
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import os
import json


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = '../../secrets/googleapi-b7fa7534555d.json'
VIEW_ID = '274858544'


def initialize_analyticsreporting():
    """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, SCOPES)

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    return analytics


def get_report(analytics, date_string):
    """Queries the Analytics Reporting API V4.

  Args:
    :param analytics: An authorized Analytics Reporting API V4 service object.
    :param date_string: date in the format 'yyyy-mm-dd', accepts also 'yesterday'
  Returns:
    The Analytics Reporting API V4 response.
  """
    return analytics.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    'dateRanges': [{'startDate': date_string, 'endDate': date_string}],
                    'metrics': [
                        {'expression': 'ga:sessions'},
                        {'expression': 'ga:users'},
                    ],
                    'dimensions': [
                        {'name': 'ga:date'},
                        {'name': 'ga:source'},
                        {'name': 'ga:channelGrouping'},
                    ]
                }]}).execute()


def make_dataframe(response):
    """
    Parses Analytics Reporting API V4 response and make a pandas dataframe

    Args:
        response: A pandas dataframe.
    """

    df_columns = []

    # parse dimensions
    for report in response.get('reports', []):
        column_header = report.get('columnHeader', {})
        dimension_headers = column_header.get('dimensions', [])
        for dim in dimension_headers:
            df_columns.append(dim)

    # parse metrics
    for report in response.get('reports', []):
        column_header = report.get('columnHeader', {})
        metric_headers = column_header.get('metricHeader', [])
        metric_header_entries = metric_headers.get('metricHeaderEntries', [])
        for metric in metric_header_entries:
            df_columns.append(metric.get('name', ''))

    # empty dataframe
    df = pd.DataFrame(columns=df_columns)

    # iterate on data
    for report in response.get('reports', []):
        for row in report.get('data', {}).get('rows', []):
            current_row = []
            for dim in row.get('dimensions', ''):
                current_row.append(dim)
            for metric in row.get('metrics', []):
                for val in metric.get('values', ''):
                    current_row.append(val)
            df.loc[len(df)] = current_row

    # rename columns
    df = df.rename(columns={
        "ga:date": "date",
        "ga:source": "source",
        "ga:channelGrouping": "channelGrouping",
        "ga:sessions": "sessions",
        "ga:users": "users",
    })

    return df


def write_data(df_in):

    print("Writing to azure...")
    # get azure access credentials
    with open("../../secrets/azure_creds.json", "r") as f:
        storage_options = json.load(f)

    # file details
    datestr = df_in["date"].iloc[0]
    filename = f"googleanalytics_{datestr}.csv"
    file_path = os.path.join("googleanalytics", filename)
    container = "staging"
    account_name = "gruppotavolastorage"

    df_in.to_csv(
        f"abfs://{container}@{account_name}.dfs.core.windows.net/{file_path}",
        index=False,
        storage_options=storage_options
    )

    print("Done!")


def main(date_string='yesterday'):
    analytics = initialize_analyticsreporting()
    response = get_report(analytics, date_string=date_string)
    df = make_dataframe(response)
    write_data(df)



if __name__ == '__main__':
    main()
