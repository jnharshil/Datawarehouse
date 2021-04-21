SPARKIFY PROJECT:

Sprakify the music streaming app wants to analyze their user data with a focus on songs that the user is listening to.
To extract this information they need the information to be retrieved from log JSON files at user end and song JSON files to combine and provide appropriate information.



The project consists of the following files:


 etl.py: 
consist of 4 functions:
 main():
 - calls the create_spark_session to create the spark session  
 - calls process_song_data() and process_log_data() function
 
 process_song_data: 
- reads the data from S3 public bucket from udacity.
- tranform the data based on pre defined schema.
- sort data and store them in parquet files locally which can be used to creat a view to run sql queries by anaylst.
 
 process_log_data:
 - consist a schema to read the s3 data.
 - reads the data from S3 public bucket from udacity.
- tranform the data based on pre defined schema.
- sort data and store them in parquet files locally which can be used to creat a view to run sql queries by anaylst.


globally define parameters;
 - config to get aws credetnials
 - song schema used in both process_song_data and process_log_data function
-  os.environ* to access s3 bucket.

to run project in terminal run python etl.py

