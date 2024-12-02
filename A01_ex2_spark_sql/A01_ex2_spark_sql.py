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
from pyspark.sql.functions import year, month, dayofmonth, to_date, date_format, first, collect_list, when
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark,
            my_dataset_dir,
            vehicle_id,
            day_picked,
            delay_limit
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

    # Filter 1: Filter by vehicleID
    filter_VehicleID = inputDF.filter((pyspark.sql.functions.col("vehicleID") == vehicle_id))

    # Filter 2: Filter by day
    # create a new column "only_days"
    filter_VehicleID_Dates = filter_VehicleID.withColumn("only_days", to_date(pyspark.sql.functions.col("date"), 'yyyy-MM-dd HH:mm:ss').cast("string"))
    # Filter the DF so that it only has rows where the only_days matches the day_picked
    filter_VehicleID_DayPicked = filter_VehicleID_Dates.filter(pyspark.sql.functions.col("only_days") == day_picked)

    # Filter 3: Does stop at station
    filter_VehicleID_DayPicked_StopsAtStation = filter_VehicleID_DayPicked.filter(pyspark.sql.functions.col("atStop") == 1)

    # get the hour-min-sec - create a column
    sort_hours = filter_VehicleID_DayPicked_StopsAtStation.withColumn("only_hours", date_format('date', 'HH:mm:ss'))

    #sort the data by the hour
    sort_date_time = sort_hours.orderBy(pyspark.sql.functions.col("only_hours").asc())

    # Track the arrival times of the the bus VehicleID at each Closest_Stop(Bus_Station_ID) for each Bus_Line
    # Group by ((bus_line, bus_stop))
    # groupBy_busLine_busStop = sort_date_time.groupBy(pyspark.sql.functions.col("busLineID"), pyspark.sql.functions.col("closerStopID"), pyspark.sql.functions.col("delay")).agg((pyspark.sql.functions.collect_list("only_hours")).alias("arrivalTime"))
    groupBy_busLine_busStop = sort_date_time.groupBy(pyspark.sql.functions.col("busLineID"), pyspark.sql.functions.col("closerStopID"), pyspark.sql.functions.col("delay")).agg((first("only_hours")).alias("arrivalTime"))

    # keeping only the first index from each group. as the index 2 are conitnuations from index 1
    #groupBy_busLine_busStop_first_index = groupBy_busLine_busStop.withColumn("arrivalTime", groupBy_busLine_busStop["arrivalTime"][0])

    # sort the data by the hour - sort the group by
    sort_arrivalTime = groupBy_busLine_busStop.orderBy(pyspark.sql.functions.col("arrivalTime").asc())


    # Add a new column "onTime" if the bus is on schedule or not. 1 - on time, 0 - not
    is_on_schedule = sort_arrivalTime.withColumn("onTime", when((pyspark.sql.functions.col("delay") >= -delay_limit) & (pyspark.sql.functions.col("delay") <= delay_limit), 1).otherwise(0))

    almost_final = is_on_schedule.select(pyspark.sql.functions.col("busLineID"), pyspark.sql.functions.col("closerStopID"), pyspark.sql.functions.col("arrivalTime"), pyspark.sql.functions.col("onTime"))

    solutionDF = almost_final.withColumnRenamed("busLineID", "lineID").withColumnRenamed("closerStopID", "stationID")

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
    vehicle_id = 33145
    day_picked = "2013-01-02"
    delay_limit = 60

    # 2. We select the Spark execution mode: Local (0), Google Colab (1) or Databricks (2)
    local_0_GoogleColab_1_databricks_2 = 1

    if (local_0_GoogleColab_1_databricks_2 == 1):
        import google.colab
        google.colab.drive.mount("/content/drive")

    elif (local_0_GoogleColab_1_databricks_2 == 2):
        import dbutils

    # 3. We select the dataset we want to work with
    #my_dataset_dir = "FileStore/tables/6_Assignments/my_dataset_complete/"
    my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex2_micro_dataset_1/"

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
            vehicle_id,
            day_picked,
            delay_limit
        )

    print("\n\n\n--- Completed ---")
