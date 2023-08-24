#API call to grab all of the acitivites within a personal account
import requests 
from pyspark.sql.types import LongType
from pyspark.sql.functions import * 
from pyspark.sql.session import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame
spark = SparkSession.builder.getOrCreate()
import pandas as pd

def activity_api_call(access_token):
    """Returns all activities for a personal strava account, need access token"""
    activities_url = "https://www.strava.com/api/v3/athlete/activities"
    header = {'Authorization': 'Bearer ' + access_token}
    param = {'per_page': 200, 'page': 1}
    activity_dataset = requests.get(activities_url, headers=header, params=param).json()
    
    return activity_dataset




def extract_activities(dataset):
    """Function to seperate activity_ids and create an activity dataframe. 
    Returns a df of only the activity ids, and another df with more details about the activiity. """

    #Empty lists for columns we want to extract
    activity_ids = []
    start_date = []
    activity_name =[]
    distance = []
    moving_time = []
    elapsed_time = []
    sport_type = []
    total_elevation_gain =[]
    count = 0

    #a while loop to iterate through the dataset and append values to lists defined above
    while count < len(dataset):
        activity_ids.append(dataset[count]['id'])
        start_date.append(dataset[count]['start_date'])
        activity_name.append(dataset[count]['name'])
        distance.append(dataset[count]['distance'])
        moving_time.append(dataset[count]['moving_time'])
        elapsed_time.append(dataset[count]['elapsed_time'])
        sport_type.append(dataset[count]['sport_type'])
        total_elevation_gain.append(dataset[count]['total_elevation_gain'])
        count += 1 
        
    #convert list to dataframe   
    #create a DF of the soley the activity_ids
    activity_id_DF = spark.createDataFrame(activity_ids, LongType())
    #add file name and timestamp
    activity_id_DF = activity_id_DF.withColumnRenamed('value', 'activity_id')\
                                    .withColumn("ingest_file_name", lit("activity_ids")) \
                                    .withColumn("ingested_at", lit(current_timestamp()))
    
    #columns names for initial DF
    #need to specify schema
    columns = ['activity_ids','start_date', 'activity_name', 'distance', 'moving_time','elapsed_time', 'sport_type'\
          ,'total_elevation_gain']
    #list of lists
    #ccombine lists of extract values into list of list
    extracted_data = [activity_ids,start_date, activity_name, distance, moving_time,elapsed_time, sport_type\
          ,total_elevation_gain]


    #create a pandas Dataframe, then convert to spark to write to storage
    #create dataframe from list of list within specified column names
    pdf = pd.DataFrame.from_dict(dict(zip(columns, extracted_data)))
    activity_df = spark.createDataFrame(pdf)

    activity_df = activity_df.withColumn("ingest_file_name", lit("activity_information")) \
                             .withColumn("ingested_at", lit(current_timestamp()))

    return activity_id_DF, activity_df    

def write_dataframe_to_storage(dataset, storage_path, option, mode ):
    """Function to write activity ids to storage. Will overwrite current delta file in storage
    Option refers to schema overwriteSchema or mergeSchema, mode being either overwrite or append"""
    dataset.write.format("delta")\
    .option(option, "true")\
    .mode(mode)\
    .save(storage_path)



def get_historical_dataset(storagepath, historical_df_to_write, historical_storagepath):
    """Retrieve record from file path, if nothing exists, insert df to write to storage"""
    try:
        #try to read from storage
        historical_dataframe = spark.read.format("delta").load(historical_storagepath)
    except: 
        try:
        #if that fails, write the submitted dataframe to storage
            write_dataframe_to_storage(historical_df_to_write,storagepath, "mergeSchema", "append" )
        finally:
            historical_dataframe = spark.read.format("delta").load(storagepath)

    return historical_dataframe

def query_segments(activity_ids : list,access_token ) -> DataFrame:
    """Gets all segment_ids for each activity_id submitted
    Returns distinct values"""
    df = pd.DataFrame()
    activity_id_list =[]
    segment_id_list =[]
    for id in activity_ids:
        activity_id_urls = ("{}{}?include_all_efforts= True").format("https://www.strava.com/api/v3/activities/",id)
        header = {'Authorization': 'Bearer ' + access_token}
        param = {'per_page': 200, 'page': 1}
        my_activity = requests.get(activity_id_urls, headers=header, params=param).json()

        segment_effort_count =  len(my_activity['segment_efforts'])
        count = 0
        while count < segment_effort_count:

            activity_id = my_activity['segment_efforts'][count]['activity']['id']
            segment_id = my_activity['segment_efforts'][count]['id']
            activity_id_list.append(activity_id)
            segment_id_list.append(segment_id)
                  
            columns = ['segment_id', 'activity_id']
            extracted_data = [segment_id_list, activity_id_list]
            segment_df = pd.DataFrame.from_dict(dict(zip(columns, extracted_data)))

            df = pd.concat([df, segment_df])

            count += 1

    #convert pandas df to spark
        
    segment_spark_df = spark.createDataFrame(df)

    #drop duplicate entries
    segment_spark_df = segment_spark_df.dropDuplicates()

    segment_spark_df = segment_spark_df.select(concat(segment_spark_df.segment_id,segment_spark_df.activity_id).alias("Activity_Segment_JointID"), 'segment_id','activity_id')

    segment_spark_df = segment_spark_df.withColumn("ingest_file_name", lit("segment_efforts_ids")) \
                                .withColumn("ingested_at", lit(current_timestamp()))

    return segment_spark_df


    