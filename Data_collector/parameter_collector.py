# importing packages
import configparser
import os
import sys
sys.path.append('..')

# reading .ini file
parameter_reader = configparser.ConfigParser()
(os.chdir("../"))

"""
This file reads all the parameters given by the user to get the processing done
"""

parameter_reader.read(os.getcwd()+"\Configurations\parameter.ini")
time_interval = parameter_reader.get("rescheduling", "time_interval_hours")
source_path = parameter_reader.get("path", "source_path")
destination_path = parameter_reader.get("path", "destination_path")
columns = parameter_reader.get("columns", "column_names")
target_column = parameter_reader.get("aggregation", "target_column")
date_column = parameter_reader.get("aggregation", "date_column")
period = parameter_reader.getint("aggregation", "period")
postgres_host= parameter_reader.get("postgres_database", "postgres_server")
postgres_database = parameter_reader.get("postgres_database", "postgres_database")
postgres_port = parameter_reader.get("postgres_database", "postgres_port")
postgres_user = parameter_reader.get("postgres_database", "postgres_username")
postgres_password = parameter_reader.get("postgres_database", "postgres_password")
postgres_data_append = parameter_reader.get("postgres_database", "postgres_data_append")
postgres_data_append = parameter_reader.get("postgres_database","postgres_data_append")
mongodb_host = parameter_reader.get("mongodb", "host")
mongodb_database = parameter_reader.get("mongodb", "database")
collection_name = parameter_reader.get("mongodb", "collection")
insert_into_postgre = parameter_reader.get("postgres_database", "insert_into_postgre")
insert_into_mongo = parameter_reader.get("mongodb", "insert_into_mongo")
mongodb_data_append = parameter_reader.get("mongodb","mongodb_data_append")
