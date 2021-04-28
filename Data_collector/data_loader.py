import os
import pandas as pd
import glob
import logging
from datetime import timedelta
from Data_collector.parameter_collector import period


def sync_data(source, destination):
    """
    This function is called by load_data() function.This function downloads data from AWS S3 bucket and stores
    it in local directory
    :param source: Source of Data - Path to AWS S3 bucket
    :param destination: local directory path to downloads and store the data from AWS S3

    """
    logging.info(" syncing data from AWS")
    os.system("aws s3 sync " + source + " " + destination)


def load_data(source, destination, date_column):
    """
    This function calls sync_data() to download data from AWs S3. loads the data and returns the data frame.
    This function can load data stores in JSON,CSV and EXCEL formats. This supports incremental aggregation( aggregates only latest data)

    :param source: Source of Data - Path to AWS S3 bucket
    :param destination: local directory path to downloads and store the data from AWS S3
    :return: dataframe of input data
    """

    sync_data(source, destination)
    df = pd.DataFrame()
    logging.info(" loading data into dataframe")
    for i in glob.glob(os.getcwd() + "/data_folder/*"):
        if i != destination + "/metadata":
            for j in glob.glob(i + "/*"):
                # identifies and loads data from json file
                if j.endswith(".json"):
                    sub_df = pd.read_json(j)
                    df = pd.concat([df, sub_df])
                # identifies and loads data from csv file
                if j.endswith(".csv"):
                    sub_df = pd.read_csv(j)
                    df = pd.concat([df, sub_df])
                # identifies and loads data from excel file
                if j.endswith(".xlsx"):
                    sub_df = pd.read_excel(j)
                    df = pd.concat([df, sub_df])
    latest_date = pd.to_datetime("01-01-1970")
    # looking out for metadata, if present avoids processing older data
    if len(glob.glob(destination + "/metadata/*")) > 0:
        for j in glob.glob(destination + "/metadata/*"):
            if j.endswith(".json"):
                data_track = pd.read_json(j)
            if j.endswith(".csv"):
                data_track = pd.read_csv(j)
            if j.endswith(".xlsx"):
                data_track = pd.read_excel(j)
        # latest date will be reduced by few days since we need aggregated data for number of days mentioned
        latest_date = pd.to_datetime(data_track.iloc[-1]['date']) - timedelta(period)

    # converting date values into timestamp format
    df[date_column] = df[date_column].apply(lambda x: pd.to_datetime(x))
    df = df[df[date_column] > (latest_date)]
    logging.info(" returned data")
    return df
