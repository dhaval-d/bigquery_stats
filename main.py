import argparse
import logging
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime


def authenticate_with_bigquery(project_id,service_account_file):
    """
    Authenticate with BigQuery client using service account credentials
    """
    credentials = service_account.Credentials.from_service_account_file(
        service_account_file,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    client = bigquery.Client(
        credentials=credentials,
        project=project_id,
    )
    return client


def create_util_dataset(client):
    """
    Create a utils dataset if it doesn't exist
    """
    dataset_id = "utils"
    # Prepares a reference to the new dataset
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)

    if client.get_dataset(dataset_ref).created is not None:
        logging.info('Dataset {} already exists.'.format(dataset.dataset_id))
        return

    # Creates the new dataset
    dataset = client.create_dataset(dataset)
    logging.info('Dataset {} created.'.format(dataset.dataset_id))


def create_daily_storage_stats_table(client):
    """
    Create daily_storage_stats table if it doesn't exist
    """
    table_id = client.project+".utils.daily_storage_stats"
    # schema for the table
    schema = [
        bigquery.SchemaField("processing_time", "datetime", mode="REQUIRED"),
        bigquery.SchemaField("project_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("dataset_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("table_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("creation_time", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("last_modified_time", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("row_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("size_bytes", "INTEGER", mode="REQUIRED"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    # create a table if it doesn't exist
    try:
        if client.get_table(table_id).created is not None:
            logging.info('Table {} already exists.'.format(table.table_id))
            return "{}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    except:
        table = client.create_table(table)  # Make an API request.
        logging.info("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
        return "{}.{}.{}".format(table.project, table.dataset_id, table.table_id)


def create_util_dataset_and_table(client):
    """
    Create dataset and a table if it doesn't exist
    """
    create_util_dataset(client)
    table = create_daily_storage_stats_table(client)
    return table


def main(project_id,service_account_file):
    """
    1. authenticate with BigQuery client
    2. Create dataset and table in BigQuery project if they don't exist
    3. Scan through all datasets in a project and store stats in a destination table
    """
    # 1. authenticate with BigQuery client
    client = authenticate_with_bigquery(project_id,service_account_file)

    # 2. Create dataset and table in BigQuery project if they don't exist
    destination_table = create_util_dataset_and_table(client)

    # 3. Scan through all datasets in a project and store stats in a destination table
    datasets = client.list_datasets()
    processing_timestamp = datetime.now()

    for dataset in datasets:
        dataset_id = dataset.full_dataset_id
        logging.info("Updating stats for : {}".format(dataset_id))

        query = str.format("INSERT `{}` (processing_time, project_id, dataset_id, table_id, "
                           "creation_time, last_modified_time, row_count, size_bytes)  ".format(destination_table)+
                           "SELECT DATETIME '{}' as processing_time,project_id, dataset_id, table_id, "
                           "creation_time, last_modified_time, row_count, size_bytes "
                           "FROM `{}.__TABLES__` where type = 1"
                           , processing_timestamp,dataset_id)
        query_job = client.query(query)

        # shouldn't return rows because
        for row in query_job:
            logging.info("{} | {} | {} | {} | {} | {} | {} | {} | {}".format(row.processing_time
                                                                              , row.project_id
                                                                              , row.dataset_id
                                                                              , row.table_id
                                                                              , row.creation_time
                                                                              , row.last_modified_time
                                                                              , row.row_count
                                                                              , row.size_bytes
                                                                              , row.type
                                                                          )
                         )


if __name__ == '__main__':
    """
    Entry point for the application. 
    --project_id: Pass project_id for which you are calculating stats
    --service_account_file: provide a service account key file location. service account needs following permissions:
        BigQuery Data Editor
        BigQuery Data Reader
        BigQuery Job user 
    Stats are stored also in the same project in {PROJECT_ID}.utils.daily_storage_stats.
    utils = dataset name
    daily_storage_stats = table name
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('--project_id', '--Please provide project_id')
    parser.add_argument('--service_account_file','--Please provide a service account key file location')
    args = parser.parse_args()

    if args.project_id is None or args.service_account_file is None:
        print 'Please provide project_id and service_account_file'
        exit(1)
    main(args.project_id,args.service_account_file)