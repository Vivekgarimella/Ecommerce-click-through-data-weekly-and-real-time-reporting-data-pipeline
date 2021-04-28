import logging

from sqlalchemy import create_engine
import pymongo
import json


def dataframe_to_postgres(data, tablename, db_append, user, password, host, port, database):
    """
    This function writes the aggregated data to postgreSQL based on credentils provided in .ini file.
    If data append is given True it appends data to already existing data, else if append is given false it replace if
    a table already exists else it will create a new table in the database
    :param data: data to be written to database
    :param tablename: desired table name for data in the database
    :param db_append: True/False - to append data to already existing table
    :param user: username
    :param password: password
    :param host: host address
    :param port: port number
    :param database: database name
    """
    logging.info("establishing connection to postgreSQL database")
    connection_string = "postgresql+psycopg2://" + str(user) + ":" + str(password) + "@" + \
                        str(host) + ":" + str(port) + "/" + str(database)
    engine = create_engine(connection_string)
    if db_append == True:
        logging.info("appending, if table exists")
        data.to_sql(tablename, con=engine, if_exists='append', index=False)
    else:
        logging.info("replacing if table exists")
        data.to_sql(tablename, con=engine, if_exists='replace', index=False)
    print("Data is uploaded to PostgreSQL")
    engine.dispose()


def dataframe_to_mongo(data,host, database_name, collection_name,mongodb_data_append):
    """
    This function writes the aggregated data to mongoDB based on credentils provided in .ini file.
        If data append is given True it appends data to already existing data, else if append is given false
        and a collection with given name is present it drops current collection and creates a new one

    :param data: data to be written to database
    :param host: host address
    :param database_name: name of the database
    :param collection_name: name of the collection
    :param mongodb_data_append: True/False - to append data to already existing collection

    """
    logging.info("Establishing connection with mongoDB")

    connection = pymongo.MongoClient("mongodb://"+host+"/")
    database = connection[database_name]
    collection = database[collection_name]
    logging.info("converting dataframe observations to dictionaries (json values)")
    rows = json.loads(data.T.to_json()).values()

    if not mongodb_data_append and collection_name in database.list_collection_names():
        logging.info("not appending, existing collection is dropped")
        collection.drop()
    collection.insert_many(rows)
    connection.close()
    print("Data Uploaded to MongoDB")
