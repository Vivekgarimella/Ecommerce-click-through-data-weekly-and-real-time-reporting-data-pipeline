# Ecommerce click through data weekly and real time reporting data pipeline


#### This pipeline is automated to handle multiple use cases.
This pipeline is designed to calculate number of times a product category and a specific product are clicked through the payment gateway(nothing but being sold) in a given period of time. This entire pipeline triggers for specific interval of time. We can specify the time interval in .ini file. This helps us to find patterns of the sales and take necessary business decisions which eventually increases the reach and revenue of Gonoodle. This pipeline is designed in such a way that it takes click events data(supports JSON,CSV and EXCEL formats) from Amazon S3, aggregates data by grouping by specific columns(there is no restriction for number of columns to be mentioned, pipeline is scalable)for given period of time( desired period of time will be mentioned in .ini file) and uploads the aggregated data back to a database (PostgreSQL/MongoDB). Source of our mock data is kaggle(*https://www.kaggle.com/tunguz/clickstream-data-for-online-shopping*).
This data is of click stream type. This data tells us if a clicked is being clicked by its users on specific dates or not. Lets consider this data as click stream data of gonoddles's ecommerce website. Let's conder this as click through successful payment gateway data, which means whenever a customer buys a product the action is being registered by the web analytics toolchain. Initially our raw-mock data consists of 6 columns: 
****user -> user ID (modified to user_id in our pipeline) 
 time_stamp -> time in epochs  (modified to date in our pipeline) 
 adgroup_id -> ad group  (modified and considered as product_category in our pipeline) 
 pid -> product ID (modified and considered as product_name in our pipeline) 
 nonclk -> shows 1 if respective item is not clicked, else 0 - (this is not necessary) 
 clk -> shows 1 respective item is clicked, else 0.****

This data will be useful to provide various weekly statistics like number of times each product or product category  is 
being clicked through the payment gateway. 
A brief description is given regarding every module of this pipeline and it's respective functions
We have 3 directories in this pipeline:
****1) Core_Logic => This folder consists of "aggregation.py" this is where entire aggregation takes place.****
****2) Data_collector => This modules consists of data_loader.py (loads data from S3),parameter_collector.py(collects all parameters mentioned in .ini file) write_to_db.py (writes data to Mongodb, PostgreSQL based on parameters given in .ini file.****
****3) Data_folder => This directories contains two sub directories :****
                :****data => click through data from s3 bucket is downloaded and stored in this folder, this data will be eleted after successfully uploading aggregated**** ****data to the database(s) metadata => this metadata folder is used only for AWS environment. AWS Lambda will be watching this folder to trigger notifications for any**** ****object creation or put events.This will be indication for our pipeline to start execution. We'll configure EC2 istance and initiate it by mentoning it in AWS Lambda,**** ****such that whenever new file is uploaded to metadata folder in S3 bycket, our datapipeline starts execution After uploading enitre data, meta data folder should be uploaded with the statistics of data uploaded. This triggers AWS Lambda, which sends a notification ****
