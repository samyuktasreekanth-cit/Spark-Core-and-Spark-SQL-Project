# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

import pyspark
import pyspark.sql.functions
import time
from pyspark.sql.functions import dayofmonth, to_date, dayofweek, hour, format_string, round
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark,
            my_dataset_dir,
            north,
            east,
            south,
            west,
            hours_list
           ):

    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("date", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("busLineID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("busLinePatternID", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("congestion", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("longitude", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("latitude", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("delay", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("vehicleID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("closerStopID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("atStop", pyspark.sql.types.IntegerType(), False)
         ])

    # 2. Operation C2: 'read'
    inputDF = spark.read.format("csv") \
        .option("delimiter", ",") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # ---------------------------------------
    # TO BE COMPLETED
    # ---------------------------------------

    # Filter 1: Filter out the weekends
    # Adding a column "day_of_week" where 1-Sun, 2-mon , 3-tue ... 7-sat
    filter_date = inputDF.withColumn("day_of_week", dayofweek(to_date(pyspark.sql.functions.col("date"), 'yyyy-MM-dd HH:mm:ss')))
    # filter out the weekends
    filter_weekday_only = filter_date.filter( (pyspark.sql.functions.col("day_of_week") >= 2) & (pyspark.sql.functions.col("day_of_week") <= 6))

    # Filter 2: hour in hours_list
    # Adding a column "hour" where it has only the HH from ( 'yyyy-MM-dd HH:mm:ss' )
    filter_hour = filter_weekday_only.withColumn("hour", hour(pyspark.sql.functions.col("date")))
    #convert int to str like in  hours_list
    filter_hour_string = filter_hour.withColumn("hour", format_string("%02d", pyspark.sql.functions.col("hour")))
    # filtering only those hours in hours_list
    filter_weekdayOnly_hourInHoursList = filter_hour_string.filter( pyspark.sql.functions.col("hour").isin(hours_list) )

    # Filter 3:  Reporting for congestion
    filter_weekdayOnly_hourInHoursList_congestion = filter_weekdayOnly_hourInHoursList.filter( pyspark.sql.functions.col("congestion") == 1 )

    # Filter 4: is within area of interest
    filter_weekdayOnly_hourInHoursList_congestion_inBounds = filter_weekdayOnly_hourInHoursList_congestion.filter( 
                                                                                                                  (pyspark.sql.functions.col("latitude") >= south) & 
                                                                                                                  (pyspark.sql.functions.col("latitude") <= north) & 
                                                                                                                  (pyspark.sql.functions.col("longitude") >= west) & 
                                                                                                                  (pyspark.sql.functions.col("longitude") <= east) )

    # Group the data by the hour - Ti - Total measurements

    # everything but congestion:
    filter_weekday_hour_in_hourslist_inbounds = filter_weekdayOnly_hourInHoursList.filter((pyspark.sql.functions.col("latitude") >= south) & 
                                                                                          (pyspark.sql.functions.col("latitude") <= north) & 
                                                                                          (pyspark.sql.functions.col("longitude") >= west) & 
       
                                                                                          (pyspark.sql.functions.col("longitude") <= east) )
    # Ti - count of each hour 
    pairedSQL = filter_weekday_hour_in_hourslist_inbounds.groupBy("hour").count().withColumnRenamed("count", "numMeasurements")

    # Ci - total congestion count in that hour
    Ci_SQL = filter_weekdayOnly_hourInHoursList_congestion_inBounds.groupBy("hour").count().withColumnRenamed("count", "congestionMeasurements")

    # joing hr, ti and ci
    # should be like: row(hour, ti, ci)
    joinedSQL = pairedSQL.join(Ci_SQL, "hour")

    # pi - percentage
    percentage = joinedSQL.withColumn("percentage", round((pyspark.sql.functions.col("congestionMeasurements") / pyspark.sql.functions.col("numMeasurements")) * 100, 2))

    # orderBy - decreasing order of Pi and (tie) - increasing order of Hi
    sortedSQL = percentage.orderBy([pyspark.sql.functions.col("percentage").desc(), pyspark.sql.functions.col("hour").asc()])

    solutionDF = sortedSQL.select(pyspark.sql.functions.col("hour"), pyspark.sql.functions.col("percentage"), pyspark.sql.functions.col("numMeasurements"), pyspark.sql.functions.col("congestionMeasurements"),)
    # ---------------------------------------

    # Operation A1: 'collect'
    resVAL = solutionDF.collect()
    for item in resVAL:
        print(item)

# --------------------------------------------------------
#
# PYTHON PROGRAM EXECUTION
#
# Once our computer has finished processing the PYTHON PROGRAM DEFINITION section its knowledge is set.
# Now its time to apply this knowledge.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer finally processes this PYTHON PROGRAM EXECUTION section, which:
# (i) Specifies the function F to be executed.
# (ii) Define any input parameter such this function F has to be called with.
#
# --------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    north = 53.3702027
    east = -6.2043634
    south = 53.3343619
    west = -6.2886331
    hours_list = ["07", "08", "09"]

    # 2. We select the Spark execution mode: Local (0), Google Colab (1) or Databricks (2)
    local_0_GoogleColab_1_databricks_2 = 1

    if (local_0_GoogleColab_1_databricks_2 == 1):
        import google.colab
        google.colab.drive.mount("/content/drive")

    elif (local_0_GoogleColab_1_databricks_2 == 2):
        import dbutils

    # 3. We select the dataset we want to work with
    #my_dataset_dir = "FileStore/tables/6_Assignments/my_dataset_complete/"
    my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex1_micro_dataset_1/"

    # 4. We set the path to my_dataset
    my_local_path = "../../../../3_Code_Examples/L07-23_Spark_Environment/"
    my_google_colab_path = "/content/drive/MyDrive/"
    my_databricks_path = "/"

    if (local_0_GoogleColab_1_databricks_2 == 0):
        my_dataset_dir = my_local_path + my_dataset_dir
    elif (local_0_GoogleColab_1_databricks_2 == 1):
        my_dataset_dir = my_google_colab_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 5. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 6. We call to our main function
    my_main(spark,
            my_dataset_dir,
            north,
            east,
            south,
            west,
            hours_list
           )

    print("\n\n\n--- Completed ---")
