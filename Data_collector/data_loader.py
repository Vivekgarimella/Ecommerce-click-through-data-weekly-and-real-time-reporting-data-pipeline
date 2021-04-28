import os
import pandas as pd
import glob

def sync_data(source, destination):

    """
    This function is called by load_data() function.This function downloads data from AWS S3 bucket and stores
    it in local directory
    :param source: Source of Data - Path to AWS S3 bucket
    :param destination: local directory path to downloads and store the data from AWS S3

    """
    os.system("aws s3 sync " + source + " " + destination)


def load_data(source,destination):
    """
    This function calls sync_data() to download data from AWs S3. loads the data and returns the data frame.
    This function can load data stores in JSON,CSV and EXCEL formats.

    :param source: Source of Data - Path to AWS S3 bucket
    :param destination: local directory path to downloads and store the data from AWS S3
    :return: dataframe of input data
    """
    sync_data(source,destination)
    df = pd.DataFrame()

    for i in glob.glob(os.getcwd()+"/data_folder/*"):
        if i != destination + "/metadata":
            for j in glob.glob(i + "/*"):
                if j.endswith(".json"):
                    sub_df = pd.read_json(j)
                    df = pd.concat([df, sub_df])
                if j.endswith(".csv"):
                    sub_df = pd.read_csv(j)
                    df = pd.concat([df, sub_df])
                if j.endswith(".xlsx"):
                    sub_df = pd.read_excel(j)
                    df = pd.concat([df, sub_df])
    return df