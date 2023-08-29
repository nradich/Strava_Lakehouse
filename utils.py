#API call to grab all of the acitivites within a personal account
import requests 
from pyspark.sql.types import LongType
from pyspark.sql.functions import * 
from pyspark.sql.session import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame, Row
spark = SparkSession.builder.getOrCreate()
import pandas as pd
from ratelimiter import RateLimiter

def activity_api_call(access_token :str):
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

def query_segments(activity_ids : list,access_token : str ) -> DataFrame:
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

def query_segment_details(segment_list: list ,access_token : str) -> DataFrame :
        "API Call to retrieve segment details ie segment distance, segment name etc. Returns a spark dataframe"

        #empty lists of values to parse from API call
        returned_segment = []
        segment_name = []
        returned_activity =[]
        activity_name = []
        activity_type = []
        segment_elevation_high =[]
        segment_elevation_low = []
        segment_distance = []
        activity_max_speed =[]
        activity_avg_speed =[]
        activity_start_date =[]
        segment_pr_date = []
        segment_pr_time=[]


        #loop through submitted segments and request segment information
        for segments in segment_list:
                segment_effort_urls = ("{}{}?include_all_efforts= True").format("https://www.strava.com/api/v3/segment_efforts/",segments)
                header = {'Authorization': 'Bearer ' + access_token}
                param = {'per_page': 200, 'page': 1}
                segments = requests.get(segment_effort_urls, headers=header, params=param).json()

                #json navigation to extract values from API call
                segment_id = segments['id']
                segment_name_extract = segments['name']
                activity_id = segments['activity']['id']
                activity_name_extract = segments['activity']['name']
                activity_type_extract = segments['activity']['type']
                elevation_high = segments['segment']['elevation_high']
                elevation_low = segments['segment']['elevation_low']
                segment_distance_extract = segments['segment']['distance']
                activity_max_speed_extract = segments['activity']['max_speed']
                activity_avg_speed_extract = segments['activity']['average_speed']
                activity_start_date_extract = segments['activity']['start_date']
                segment_pr_date_extract = segments['athlete_segment_stats']['pr_date']
                segment_pr_time_extract = segments['athlete_segment_stats']['pr_elapsed_time']

                #Append extracted values to lists
                returned_segment.append(segment_id)
                segment_name.append(segment_name_extract)
                returned_activity.append(activity_id)
                activity_name.append(activity_name_extract)
                activity_type.append(activity_type_extract)
                segment_elevation_high.append(elevation_high)
                segment_elevation_low.append(elevation_low)
                segment_distance.append(segment_distance_extract)
                activity_max_speed.append(activity_max_speed_extract)
                activity_avg_speed.append(activity_avg_speed_extract)
                activity_start_date.append(activity_start_date_extract)    
                segment_pr_date.append(segment_pr_date_extract)
                segment_pr_time.append(segment_pr_time_extract)



                columns = ['returned_segment','segment_name','returned_activity' ,'activity_name' ,'activity_type' ,'segment_elevation_high' ,
                        'segment_elevation_low' ,'segement_distance' ,'activity_max_speed','activity_avg_speed','activity_start_date', 
                        'segment_pr_date' ,'segment_pr_time']

                extracted_data = [returned_segment,segment_name,returned_activity,activity_name,activity_type ,
                                        segment_elevation_high,segment_elevation_low,segment_distance ,activity_max_speed,
                                        activity_avg_speed,activity_start_date,segment_pr_date ,segment_pr_time ]

                #combined extracted values and associated columns names to a DF
                segment_data_df = pd.DataFrame.from_dict(dict(zip(columns, extracted_data)))

                #convert pandas DF to spark DF
                segment_spark_df = spark.createDataFrame(segment_data_df)
                segment_spark_df = segment_spark_df.withColumn("ingest_file_name", lit("segment_details")) \
                                .withColumn("ingested_at", lit(current_timestamp()))
        return segment_spark_df

def list_comparison(subset :list, all_entries :list )-> list:
    "Compares two list and returns entries found in all entries, but not in subset"
    not_in_subset = [x for x in all_entries if x not in subset ]
    return not_in_subset

def append_activities_without_segments( segment_df : DataFrame, activites_no_segments : list ) -> DataFrame:
    "Append in activies that do not return any segments from the API call"
    rows = [Row(Activity_Segment_JointID=i,  activity_id = i) for i in activites_no_segments]
    new_df = spark.createDataFrame(rows)
    new_df = new_df.withColumn("ingest_file_name", lit("segment_efforts_ids")) \
                                    .withColumn("ingested_at", lit(current_timestamp()))\
                                    .withColumn("segment_id", lit(None).cast("long"))

    #reorder columns to union into 
    new_df_reordered = new_df.select(*segment_df.columns)

    #union the two datasets together
    all_segment_ids = segment_df.union(new_df_reordered)

    return all_segment_ids

def query_segment_details_with_limits(segment_list: list ,access_token : str, rate_limiter) -> DataFrame :
        "API Call to retrieve segment details ie segment distance, segment name etc. Returns a spark dataframe"

        #empty lists of values to parse from API call
        returned_segment = []
        segment_name = []
        returned_activity =[]
        activity_name = []
        activity_type = []
        segment_elevation_high =[]
        segment_elevation_low = []
        segment_distance = []
        activity_max_speed =[]
        activity_avg_speed =[]
        activity_start_date =[]
        segment_pr_date = []
        segment_pr_time=[]

                #loop through submitted segments and request segment information
        for segments in segment_list:
                with rate_limiter:
                    try:
                        segment_effort_urls = ("{}{}?include_all_efforts= True").format("https://www.strava.com/api/v3/segment_efforts/",segments)
                        header = {'Authorization': 'Bearer ' + access_token}
                        param = {'per_page': 200, 'page': 1}
                        segments = requests.get(segment_effort_urls, headers=header, params=param).json()

                        #json navigation to extract values from API call
                        segment_id = segments['id']
                        segment_name_extract = segments['name']
                        activity_id = segments['activity']['id']
                        activity_name_extract = segments['activity']['name']
                        activity_type_extract = segments['activity']['type']
                        elevation_high = segments['segment']['elevation_high']
                        elevation_low = segments['segment']['elevation_low']
                        segment_distance_extract = segments['segment']['distance']
                        activity_max_speed_extract = segments['activity']['max_speed']
                        activity_avg_speed_extract = segments['activity']['average_speed']
                        activity_start_date_extract = segments['activity']['start_date']
                        segment_pr_date_extract = segments['athlete_segment_stats']['pr_date']
                        segment_pr_time_extract = segments['athlete_segment_stats']['pr_elapsed_time']

                        #Append extracted values to lists
                        returned_segment.append(segment_id)
                        segment_name.append(segment_name_extract)
                        returned_activity.append(activity_id)
                        activity_name.append(activity_name_extract)
                        activity_type.append(activity_type_extract)
                        segment_elevation_high.append(elevation_high)
                        segment_elevation_low.append(elevation_low)
                        segment_distance.append(segment_distance_extract)
                        activity_max_speed.append(activity_max_speed_extract)
                        activity_avg_speed.append(activity_avg_speed_extract)
                        activity_start_date.append(activity_start_date_extract)    
                        segment_pr_date.append(segment_pr_date_extract)
                        segment_pr_time.append(segment_pr_time_extract)


                    except requests.RequestException as e:
                        print(f"API call failed with exception: {e}")
                        return None



        columns = ['returned_segment','segment_name','returned_activity' ,'activity_name' ,'activity_type' ,'segment_elevation_high' ,
                'segment_elevation_low' ,'segement_distance' ,'activity_max_speed','activity_avg_speed','activity_start_date', 
                'segment_pr_date' ,'segment_pr_time']

        extracted_data = [returned_segment,segment_name,returned_activity,activity_name,activity_type ,
                                segment_elevation_high,segment_elevation_low,segment_distance ,activity_max_speed,
                                activity_avg_speed,activity_start_date,segment_pr_date ,segment_pr_time ]

        #combined extracted values and associated columns names to a DF
        segment_data_df = pd.DataFrame.from_dict(dict(zip(columns, extracted_data)))

        #convert pandas DF to spark DF
        segment_spark_df = spark.createDataFrame(segment_data_df)
        segment_spark_df = segment_spark_df.withColumn("ingest_file_name", lit("segment_details")) \
                        .withColumn("ingested_at", lit(current_timestamp()))
        return segment_spark_df
        




