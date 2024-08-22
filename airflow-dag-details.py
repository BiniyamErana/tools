#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# File     : airflow-dag-details.py
# @Author  : Biniyam Erana
# @Contact : 
# @Created : May 23, 2024
# @Version : '1.0'
# @License :
# @Desc    : Get Airflow DAG Details
# ---------------------------------------------------------------------------

import os
import re
import sys
import json
import pytz
import logging
import warnings
import requests
import argparse
import urllib.parse
import pandas as pd
from datetime import datetime, date
from asyncio import exceptions
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import secretmanager

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../adc.json"


class AirflowDagDetails(object):

    def __init__(self, args, bq_client, storage_client, secretmanager_client):
        self.args = args
        self.bq_client = bq_client
        self.storage_client = storage_client
        self.secretmanager_client = secretmanager_client
        self.today = date.today().strftime('%Y-%m-%d')
        self.timestamp = datetime.now()
        self.payload = {}
        self.headers = {
            "Accept": "application/json",
            'Authorization': f'Bearer {self.get_secret(self.args.project, self.args.secret_id)}'
        }

    def get_secret(self, project_id, secret_id, version_id='latest'):
        client = self.secretmanager_client

        # Build the resource name of the secret version
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        # Access the secret version
        response = client.access_secret_version(name=name)
        secret_value = response.payload.data.decode('UTF-8')

        return secret_value

    def convert_date_format(self, date_str):
        dt = None
        input_formats = [
            '%Y-%m-%d %H:%M:%S %Z',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f %Z',
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%Y-%m-%dT%H:%M:%S.%f000'
        ]

        for fmt in input_formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                break
            except ValueError:
                continue

        # If no format matched, raise an error
        if dt is None:
            raise ValueError(f"Date string '{date_str}' does not match any expected format")

        dt = dt.replace(tzinfo=pytz.UTC)
        # Convert the datetime object to the desired format
        output_format = '%Y-%m-%dT%H:%M:%SZ'
        formatted_date = dt.strftime(output_format)
        encoded_date = urllib.parse.quote(formatted_date, safe='')

        return encoded_date

    def get_full_url(self, base_url, dag_runs, limit=1000, offset=0, dag_name=None, filter_date=None):
        if base_url.endswith("/"):
            base_url = base_url[:-1]

        if dag_runs:
            dag_name = dag_name.lstrip('/')
            if filter_date:
                url_format = f"{base_url}/{dag_name}/dagRuns?limit={limit}&offset={offset}&execution_date_gte={filter_date}"
            else:
                url_format = f"{base_url}/{dag_name}/dagRuns?limit={limit}&offset={offset}"
            return url_format.format(
                base_url=base_url,
                dag_name=dag_name,
                filter_date=filter_date)
        else:
            return "{base_url}?limit={limit}&offset={offset}".format(
                base_url=base_url,
                limit=limit,
                offset=offset)

    # Flatten the dictionary
    def flatten_dict(self, d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                if all(isinstance(i, dict) for i in v):
                    for i, item in enumerate(v):
                        items.extend(self.flatten_dict(item, f"{new_key}{sep}{i}", sep=sep).items())
                else:
                    items.append((new_key, v))
            else:
                items.append((new_key, v))
        return dict(items)

    def clean_column(self, value):
        value_str = str(value)
        value_str = value_str.replace('[', '').replace(']', '').replace("'", "")
        value_str = value_str.strip()
        value_str = value_str.strip(',')

        return value_str

    def clean_df(self, df_final_result):
        current_timestamp = self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        df_final_result['run_ts'] = current_timestamp
        df_final_result = df_final_result.where(pd.notna(df_final_result), '')
        df_final_result.fillna('')
        df_final_result.drop_duplicates(inplace=True)

        return df_final_result

    def make_api_request(self, full_url):
        response = requests.request("GET", full_url, headers=self.headers, data=self.payload, verify=False)
        response.raise_for_status()  # Raise an exception if the response is not successful

        return response

    def get_dag_details(self):
        leng_dag_dict, offset = 0, 0
        processed_data = []

        # List of keys to drop
        keys_to_drop = [
            "file_token", "fileloc", "last_expired", "last_parsed_time", "tags",
            "last_pickled", "next_dagrun_create_after", "next_dagrun_data_interval_end",
            "next_dagrun_data_interval_start", "pickle_id", "root_dag_id", "scheduler_lock"
        ]

        while offset <= leng_dag_dict:
            try:
                full_url = self.get_full_url(base_url=self.args.url, dag_runs=False, offset=offset)
                response = self.make_api_request(full_url)
                logging.info("URL for Dags: {}".format(full_url))

                # Parse the JSON string into a dictionary
                data_list = json.loads(response.text)

                for data_dict in data_list["dags"]:
                    # Drop the unwanted keys
                    for key in keys_to_drop:
                        if key in data_dict:
                            del data_dict[key]
                    # Flatten the dictionary
                    flat_data_dict = self.flatten_dict(data_dict)
                    processed_data.append(flat_data_dict)

                leng_dag_dict = data_list["total_entries"]
                offset += 100

            except requests.exceptions.RequestException as e:
                logging.error("Error occurred while making the request: {}".format(e))
            except json.JSONDecodeError as e:
                logging.error("Error occurred while parsing the response: {}".format(e))
            except KeyError as e:
                logging.error("Error occurred while accessing the dictionary: {}".format(e))
            except Exception as e:
                logging.error("An unexpected error occurred: {}".format(e))

        logging.info("total_entries Dags: {}".format(leng_dag_dict))

        df_final_result = pd.DataFrame(processed_data)

        if len(df_final_result) > 0:
            del df_final_result["schedule_interval"]
            df_final_result.rename(columns={'schedule_interval___type': 'schedule_interval_type'}, inplace=True)
            df_final_result['owners'] = df_final_result['owners'].apply(self.clean_column)
            df_final_result = self.clean_df(df_final_result)

        return df_final_result

    def get_dag_runs(self, df):
        filter_date = f"{self.args.filter_date}T00%3A00%3A00Z"  # "2024-05-01"
        processed_data = []
        df_bq_dag_runs = pd.DataFrame()
        # List of keys to drop
        keys_to_drop = [
            "data_interval_end", "data_interval_start", "note", "last_scheduling_decision", "logical_date", "conf"
        ]
        if self.args.dataset and self.args.table_dag_details and self.args.table_dag_runs:
            df_bq_dag_runs = self.read_from_bigquery(table_name=self.args.table_dag_runs)

        for idx, value in df['dag_id'].items():
            leng_dag_dict, offset = 0, 0
            while offset <= leng_dag_dict:
                try:
                    filtered_df = df_bq_dag_runs[df_bq_dag_runs['dag_id'] == value] if not df_bq_dag_runs.empty else pd.DataFrame()
                    if not filtered_df.empty:
                        execution_date = str(filtered_df['execution_date'].values[0])
                        filter_date = self.convert_date_format(execution_date)

                    full_url = self.get_full_url(base_url=self.args.url, dag_runs=True, offset=offset, dag_name=value, filter_date=filter_date)
                    response = self.make_api_request(full_url)
                    logging.info("URL for Dag RUNs: {}".format(full_url))

                    # Parse the JSON string into a dictionary
                    data_list = json.loads(response.text)

                    for data_dict in data_list["dag_runs"]:
                        # Drop the unwanted keys
                        for key in keys_to_drop:
                            if key in data_dict:
                                del data_dict[key]
                        # Flatten the dictionary
                        flat_data_dict = self.flatten_dict(data_dict)
                        processed_data.append(flat_data_dict)

                    leng_dag_dict = data_list["total_entries"]
                    offset += 100

                except requests.exceptions.RequestException as e:
                    logging.error("Error occurred while making the request: {}".format(e))
                except json.JSONDecodeError as e:
                    logging.error("Error occurred while parsing the response: {}".format(e))
                except KeyError as e:
                    logging.error("Error occurred while accessing the dictionary: {}".format(e))
                except Exception as e:
                    logging.error("An unexpected error occurred: {}".format(e))

        # Convert the list of flattened dictionaries to a DataFrame
        df_final_result = pd.DataFrame(processed_data)
        if len(df_final_result) > 0:
            df_final_result = self.clean_df(df_final_result)

        logging.info("total_entries Dag Runs: {}".format(len(df_final_result)))

        return df_final_result

    def save_into_gcs(self, df, dag_details_path):
        df.to_csv(dag_details_path, index=False, header=True)

    def read_from_bigquery(self, table_name):
        table_ref = self.bq_client.dataset(self.args.dataset).table(table_name)

        # Load the table data into a DataFrame
        query = f"SELECT * FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY dag_id ORDER BY start_date DESC NULLS LAST) as rnk FROM `{table_ref}`) WHERE rnk = 1"
        df_result = self.bq_client.query(query).to_dataframe()

        return df_result

    def insert_into_bigquery(self, df, table_name, write_type='overwrite'):
        df['run_dt'] = self.today
        table_ref = self.bq_client.dataset(self.args.dataset).table(table_name)

        table = self.bq_client.get_table(table_ref)
        # Get the schema of the existing table
        table_schema = table.schema

        # Convert DataFrame to match the schema of the existing table
        for field in table_schema:
            if field.name in df.columns:
                if field.field_type == 'INTEGER':
                    df[field.name] = df[field.name].astype('int64')
                elif field.field_type == 'FLOAT':
                    df[field.name] = df[field.name].astype('float64')
                elif field.field_type == 'STRING':
                    df[field.name] = df[field.name].astype('str')
                elif field.field_type == 'BOOL':
                    df[field.name] = df[field.name].astype('bool')
                elif field.field_type == 'TIMESTAMP':
                    df[field.name] = pd.to_datetime(df[field.name], errors='coerce')
                elif field.field_type == 'DATE':
                    df[field.name] = pd.to_datetime(df[field.name], errors='coerce').dt.date

        write_disposition = bigquery.WriteDisposition.WRITE_APPEND if write_type == 'append' else bigquery.WriteDisposition.WRITE_TRUNCATE

        # WRITE_APPEND appends the data to the table
        # WRITE_TRUNCATE overwrites the table if it already exists
        job_config = bigquery.LoadJobConfig(
            schema=table_schema,
            write_disposition=write_disposition
        )

        job = self.bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        # Wait for the load job to complete
        job.result()

    def run(self):
        today = self.today
        current_timestamp = self.timestamp.strftime("%Y-%m-%d_%H%M")
        write_type = 'append'

        if self.args.dag_runs:
            logging.info("Dag Runs: {}".format(self.args.dag_runs))
            dag_details_path = f"{self.args.output_dir}/dag-details/run_dt={today}/airflow-dag-details_{current_timestamp}.csv"
            dag_runs_path = f"{self.args.output_dir}/dag-runs/run_dt={today}/airflow-dag-runs_{current_timestamp}.csv"

            dag_details_df = self.get_dag_details()
            dag_runs_df = self.get_dag_runs(dag_details_df)

            self.save_into_gcs(dag_details_df, dag_details_path)
            self.save_into_gcs(dag_runs_df, dag_runs_path)

            if self.args.dataset and self.args.table_dag_details and self.args.table_dag_runs:
                self.insert_into_bigquery(dag_details_df, table_name=self.args.table_dag_details)
                self.insert_into_bigquery(dag_runs_df, table_name=self.args.table_dag_runs, write_type=write_type)
            else:
                logging.info("You need to provide dataset and table name to insert into BigQuery.")

        else:
            logging.info("Dag Runs: {}".format(self.args.dag_runs))
            dag_details_path = f"{self.args.output_dir}/dag-details/run_dt={today}/airflow-dag-details_{current_timestamp}.csv"
            dag_details_df = self.get_dag_details()
            self.save_into_gcs(dag_details_df, dag_details_path)
            if self.args.dataset and self.args.table_dag_details:
                self.insert_into_bigquery(dag_details_df, table_name=self.args.table_dag_details)
            else:
                logging.info("You need to provide dataset and table name to insert into BigQuery.")


def initialize_logging():
    """
    Set up logging handlers.
    """
    logging.getLogger('Airflow_DAG_Details')
    warnings.filterwarnings('ignore')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s: %(message)s')


def configure_run_parser(run_parser):
    run_parser.set_defaults(func=AirflowDagDetails.run)
    run_parser.add_argument(
        '-u',
        '--url',
        required=True,
        default=None,
        help='Astronomer Airflow URL.')
    run_parser.add_argument(
        '-s',
        '--secret_id',
        required=True,
        default=None,
        help='Secret Id')
    run_parser.add_argument(
        '-r',
        '--dag_runs',
        required=False,
        default=False,
        action="store_true",
        help='Include Scheduled Dag Runs.')
    run_parser.add_argument(
        '-f',
        '--filter_date',
        required=False,
        default=None,
        help='Filter Date Range for Scheduled Dag Runs.')
    run_parser.add_argument(
        '-p',
        '--project',
        required=True,
        default=None,
        help='Destination Project ID.')
    run_parser.add_argument(
        '-d',
        '--dataset',
        required=False,
        default=None,
        help='Destination BigQuery Dataset ID.')
    run_parser.add_argument(
        '-td',
        '--table_dag_details',
        required=False,
        default=None,
        help='Destination BigQuery DAG Details Table ID.')
    run_parser.add_argument(
        '-tr',
        '--table_dag_runs',
        required=False,
        default=None,
        help='Destination BigQuery DAG Runs Table ID.')
    run_parser.add_argument(
        '-o',
        '--output_dir',
        required=True,
        default=None,
        help='Destination Output Location.')


def create_argparser():
    parser = argparse.ArgumentParser(prog='Airflow_Dag_Details')
    configure_run_parser(parser)

    return parser


def parse_args(input_args):
    parser = create_argparser()
    return parser.parse_args(input_args)


def main():
    input_args = re.sub(r'\s+', '', sys.argv[1].strip()).replace("'", "").replace("=", ",").split(',')
    args = parse_args(input_args)
    initialize_logging()
    print(args)
    try:
        PROJECT_ID = args.project
        bq_client = bigquery.Client(project=PROJECT_ID)
        storage_client = storage.Client(project=PROJECT_ID)
        secretmanager_client = secretmanager.SecretManagerServiceClient()
        dag_details = AirflowDagDetails(args, bq_client, storage_client, secretmanager_client)
        dag_details.run()
        bq_client.close()
        storage_client.close()
    except exceptions as ex:
        logging.critical('Encountered error: {}'.format(ex))
        sys.exit(1)


if __name__ == "__main__":

    main()
