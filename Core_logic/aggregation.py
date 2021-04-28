# importing required packages.
import configparser
import os
import logging
import pandas as pd
from datetime import timedelta
import sys
from apscheduler.schedulers.blocking import BlockingScheduler

# importing required functions and constants from other sections of the pipeline
sys.path.append("..")
from Data_collector.data_loader import load_data
from Data_collector.parameter_collector import source_path, \
    destination_path, target_column, date_column, period, postgres_port, postgres_user, postgres_password, \
    columns, postgres_host, postgres_database, postgres_data_append, mongodb_database, mongodb_host, \
    collection_name, insert_into_postgre, insert_into_mongo, mongodb_data_append,time_interval
from Data_collector.write_to_DB import dataframe_to_postgres, dataframe_to_mongo

""" 
This pipeline is automated to handle multiple use cases.
This pipeline is designed to calculate number of times a product category and a specific product are clicked through the
payment gateway(nothing but being sold) in a given period of time. This entire pipeline triggers for specific interval
 of time. We can specify the time interval in .ini file. This helps us to find patterns of the sales and 
take necessary business decisions which eventually increases the reach and revenue of Gonoodle.  
This pipeline is designed in such a way that it takes click events data(supports JSON,CSV and EXCEL formats) from 
Amazon S3, aggregates data by grouping by specific columns(there is no restriction for number of columns to be mentioned, pipeline is
scalable)for given period of time( desired period of time will be mentioned in .ini file) and uploads
the aggregated data back to a database (PostgreSQL/MongoDB).  
Source of our mock data is kaggle(https://www.kaggle.com/tunguz/clickstream-data-for-online-shopping).
This data is of click stream type. This data tells us if a clicked is being clicked by its users on specific dates or 
not. Lets consider this data as click stream data of gonoddles's ecommerce website. Let's conder this as click through 
successful payment gateway data, which means whenever a customer buys a product the action is being registered by the 
web analytics toolchain. Initially our raw-mock data consists of 6 columns: 
user -> user ID (modified to user_id in our pipeline)
time_stamp -> time in epochs  (modified to date in our pipeline)
adgroup_id -> ad group  (modified and considered as product_category in our pipeline)
pid -> product ID (modified and considered as product_name in our pipeline)
nonclk -> shows 1 if respective item is not clicked, else 0 - (this is not necessary)
clk -> shows 1 respective item is clicked, else 0.

This data will be useful to provide various weekly statistics like number of times each product or product category  is 
being clicked through the payment gateway. 
A brief description is given regarding every module of this pipeline and it's respective functions
We have 3 directories in this pipeline:
1) Core_Logic => This folder consists of "aggregation.py" this is where entire aggregation takes place.
2) Data_collector => This modules consists of data_loader.py (loads data from S3),
                    parameter_collector.py(collects all parameters mentioned in .ini file)
                    write_to_db.py (writes data to Mongodb, PostgreSQL based on parameters given in .ini file.
3) Data_folder => This directories contains two sub directories :
                data => click through data from s3 bucket is downloaded and stored in this folder, this data will be 
                deleted after successfully uploading aggregated data to the database(s).
                metadata => this metadata folder is used only for AWS environment. AWS Lambda wil be watching this 
                folder to trigger notifications for any object creation or put events.This will be indication for our 
                pipeline to start execution. We'll configure EC2 istance and initiate it by mentoning it in AWS Lambda, 
                such that whenever new file is uploaded to metadata folder in S3 bycket, our data pipeline starts execution 
                After uploading enitre data, meta data folder should be uploaded with the statistics of data uploaded. 
                This triggers AWS Lambda, which sends a notification.
                While on local enviroment we store a json file with latest date from past data, this helps in incremental
                aggregation 
"""


def fill_missing_dates(data, column, date_column):
    """ This function is called from aggregated_data function.This function is used to generate observations with
        missing dates in given data. Since we're aggreation scores for a given period of time this is a crucial step to
        implement. This function takes data, columns we want to groupby, date column as input and returns data set by
        concatenating missing dates"""
    """
    :param data: input data
    :param column: column to be groupedby before aggregating
    :param date_column: column name of date column
    :return: data without gaps within dates
    """
    logging.info("converting date to timestamp format")

    # considering only first occurence of duplicated dates in our data and resampling to get missing dates.
    logging.info("resampling the dates")
    missing_data = data.groupby([column]).apply(lambda x: x.set_index(date_column).resample('1D').first().reset_index())

    # setting date column as index beacuse we cannot do reset_index as groupby column is present in index as well as
    # normal columns due to previous step. now date becomes the index
    logging.info("setting date as index as we cannot reset index ")
    missing_data = missing_data.set_index(date_column)

    # resetting the index
    logging.info("reindexing")
    missing_data = missing_data.reset_index()

    # concatenating the original data and resampled data
    final_data = pd.concat([data, missing_data])

    # removing duplicates
    logging.info("removing duplicates")
    final_data = final_data[~final_data.duplicated()]
    return final_data


def aggregated_data(data, column, date_column, target_column, period):
    """This function aggregates the data based on period of time mentioned in .ini file."""
    """
    
    :param data: data to be aggregated
    :param column: main column to be groupedby 
    :param date_column: column name of date column
    :param target_column: column whose values are being aggregated
    :param period: time period for which data needs to be aggregated
    :return: 
    """

    # resamples the data and fills missing dates in the data
    logging.info("calling missing dates function")
    data = fill_missing_dates(data, column, date_column)

    # aggregating number of clicks in a day
    logging.info("aggregating for each day")
    data = data.groupby([column, date_column])[target_column].sum().reset_index()
    # aggregating number of clicks in specified period of time
    logging.info("aggregating for given period of time")
    data = data.set_index([column, date_column]).groupby(level=[0])['click_event'].apply(
        lambda x: x.rolling(min_periods=1, window=period).sum()).reset_index()

    return data


def identify_columns_to_aggregate(date_column):
    """

    :param date_column: date column
    :return: Tru/False - incdicating ducessfull uploading of data to database
    """
    """This function identifies the columns to groupby the data on, calls the functions that aggregates the data for
    each of column mentioned and uploads data to database(s). If data is uploaded successfully it returns True"""

    # checking we need to aggregate weekly data for more than one. This script is scalable, can taken dynamic inputs
    logging.info("Identify columns to groupby")
    if ',' in columns:
        # identifies if number of columns mentioed is greater than 1 by looking for comma"," in the string
        columns_to_aggregate = columns.split(',')
        logging.info("multiple columns given")
    else:
        # only one column to aggregate
        columns_to_aggregate = [columns]
        logging.info("single column given")
    # load data into a dataframe once downloaded from AWS S3 bucket
    df = load_data(source_path, destination_path,date_column)

    # ietrated through mentioned columns
    logging.info("applying group by for each column specified ")
    for i in range(0, len(columns_to_aggregate)):
        logging.info(" currently processing column : "+columns_to_aggregate[i])
        # aggregates data for given period of time
        periodic_aggregation = aggregated_data(df, columns_to_aggregate[i], date_column, target_column, period)

        # calculates startdate of observation
        periodic_aggregation['start_date'] = periodic_aggregation['date'].apply(lambda x: x - timedelta(period))
        # writes data to postgreSQL based on input giveb in parameter.ini


        if insert_into_postgre == True:
            logging.info("opted to write data into PostgreSQL")
            dataframe_to_postgres(df, "aggregated_table_" + str(period), postgres_data_append, postgres_user,
                                  postgres_password, postgres_host, postgres_port, postgres_database)
        # writes data to postgreSQL based on input giveb in parameter.ini
        if insert_into_mongo == True:
            logging.info("Opted to write data into MongoDB")
            dataframe_to_mongo(df, mongodb_host, mongodb_database, collection_name, mongodb_data_append)
    latest_date_data = periodic_aggregation.copy()
    latest_date_data['date'] = max(periodic_aggregation['date'])
    latest_date_data = latest_date_data.loc[:2]
    latest_date_data = latest_date_data.rename(columns={date_column: "date"})
    latest_date_data.to_csv("s3://ecommmerce/metadata/latest_date_data.csv", index=False)
    return True


def delete_data():
    """   This function deletes data_folder after successful upload of aggregated data to database(s) """
    os.system("rmdir /Q /S data_folder")
    print(" Downloaded files are deleted")


def initiate_processing():
    """ This function is called by scheduler which invokes entire pipeline for every time interval that has been set."""
    logging.info("initiate processing triggered")
    parameter_reader = configparser.ConfigParser()
    date_column = parameter_reader.get("aggregation", "date_column")
    data_upload = identify_columns_to_aggregate(date_column)
    # if aggregated data is uploaded succesfully to database(s), imput files downloaded from AWS S3 are deleted
    if data_upload == True:
        logging.info("deleting data from local")
        delete_data()


if __name__ == '__main__':
    logging.basicConfig(filename="logging_file",
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.INFO)
    # scheduler is initiated
    logging.info("configuring scheduler")
    scheduler = BlockingScheduler()
    scheduler.add_job(initiate_processing, 'interval', hours=float(time_interval))
    logging.info("Scheduler initiated")
    scheduler.start()
