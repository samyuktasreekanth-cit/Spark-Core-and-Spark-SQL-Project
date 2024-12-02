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
import datetime
from pyspark.sql.functions import sort_array, size, max

# ------------------------------------------
# FUNCTION get_ub_time
# ------------------------------------------
def get_ub_time(current_time, seconds_horizon):
    # 1. We create the output variable
    res = current_time

    # 2. We convert the current time to a timestamp
    my_tuple = datetime.datetime.strptime(current_time,"%Y-%m-%d %H:%M:%S").timetuple()
    my_timestamp = time.mktime(my_tuple)

    # 3. We update the new timestamp with the seconds to be added
    my_timestamp += seconds_horizon

    # 4. We revert it back to current time mode
    my_datetime = datetime.datetime.fromtimestamp(int(my_timestamp))
    res = my_datetime.strftime("%Y-%m-%d %H:%M:%S")

    # 5. We return res
    return res


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, my_dataset_dir, current_time, seconds_horizon):
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

    # 3. We get the ub time
    ub_time = get_ub_time(current_time, seconds_horizon)

    # ---------------------------------------
    # TO BE COMPLETED
    # ---------------------------------------

    # Filter 1: Time window
    # Converting the first dat_time column to a timestamp than str
    date_time_timestamp = inputDF.withColumn("date", pyspark.sql.functions.to_timestamp("date", "yyyy-MM-dd HH:mm:ss"))
    filter_timeWindow = date_time_timestamp.filter((pyspark.sql.functions.col("date") >= current_time) & (pyspark.sql.functions.col("date") <= ub_time))


    # Filter 2: at_stop_stop = 1
    filter_timeWindow_atStop = filter_timeWindow.filter((pyspark.sql.functions.col("atStop")) == 1)

    # (group by)key: stationID - closest station
    # (agg)value - vehicle ids
    #(stationID, vehicleID) ...
    # Also the use of collect_sets eliminates duplicates
    groupedSQL = filter_timeWindow_atStop.groupBy("closerStopID").agg(pyspark.sql.functions.collect_set("vehicleID").alias("vehicleIDs"))

    # Sort by increading order of VehicleID 
    # Row (279, [33000, 33145])
    # replace existing list with new sorted one
    sort_vehicleID_contents = groupedSQL.withColumn("vehicleIDs", sort_array("vehicleIDs"))

    # get how many vehicles are there in the list
    # Row(closerStopID=500, vehicleIDs=[28000, 33145], numVehicles=2)
    get_max_size_len_vehicleID = sort_vehicleID_contents.withColumn("numVehicles", size("vehicleIDs"))

    max_size = get_max_size_len_vehicleID.select(max("numVehicles")).first()[0]
    preSorted = get_max_size_len_vehicleID.filter(pyspark.sql.functions.col("numVehicles") == max_size)

    #sort by asc order of stationID
    preSelected = preSorted.orderBy(pyspark.sql.functions.col("closerStopID").asc())

    selectedColumns = preSelected.select(pyspark.sql.functions.col("closerStopID"), pyspark.sql.functions.col("vehicleIDs"))

    solutionDF = selectedColumns.withColumnRenamed("closerStopID", "stationID").withColumnRenamed("vehicleIDs", "sortedvehicleIDList")

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
    current_time = "2013-01-07 06:30:00"
    seconds_horizon = 1800

    # 2. We select the Spark execution mode: Local (0), Google Colab (1) or Databricks (2)
    local_0_GoogleColab_1_databricks_2 = 1

    if (local_0_GoogleColab_1_databricks_2 == 1):
        import google.colab
        google.colab.drive.mount("/content/drive")

    elif (local_0_GoogleColab_1_databricks_2 == 2):
        import dbutils

    # 3. We select the dataset we want to work with
    my_dataset_dir = "FileStore/tables/6_Assignments/my_dataset_complete/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex3_micro_dataset_1/"

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
            current_time,
            seconds_horizon
           )

    print("\n\n\n--- Completed ---")
