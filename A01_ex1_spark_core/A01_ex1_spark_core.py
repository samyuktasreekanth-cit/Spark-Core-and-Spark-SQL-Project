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
import time
from datetime import datetime
from decimal import Decimal
# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We get the parameter list from the line
    params_list = line.strip().split(",")

    #(00) Date => The date of the measurement. String <%Y-%m-%d %H:%M:%S> (e.g., "2013-01-01 13:00:02").
    #(01) Bus_Line => The bus line. Int (e.g., 120).
    #(02) Bus_Line_Pattern => The pattern of bus stops followed by the bus. String (e.g., "027B1001"). It can be empty (e.g., "").
    #(03) Congestion => On whether the bus is at a traffic jam (No -> 0 and Yes -> 1). Int (e.g., 0).
    #(04) Longitude => Longitude position of the bus. Float (e.g., -6.269634).
    #(05) Latitude = > Latitude position of the bus. Float (e.g., 53.360504).
    #(06) Delay => Delay of the bus in seconds (negative if ahead of schedule). Int (e.g., 90).
    #(07) Vehicle => An identifier for the bus vehicle. Int (e.g., 33304)
    #(08) Closer_Stop => An idenfifier for the closest bus stop given the current bus position. Int (e.g., 7486). It can be no bus stop, in which case it takes value -1 (e.g., -1).
    #(09) At_Stop => On whether the bus is currently at the bus stop (No -> 0 and Yes -> 1). Int (e.g., 0).

    # 3. If the list contains the right amount of parameters
    if (len(params_list) == 10):
        # 3.1. We set the right type for the parameters
        params_list[1] = int(params_list[1])
        params_list[3] = int(params_list[3])
        params_list[4] = float(params_list[4])
        params_list[5] = float(params_list[5])
        params_list[6] = int(params_list[6])
        params_list[7] = int(params_list[7])
        params_list[8] = int(params_list[8])
        params_list[9] = int(params_list[9])

        # 3.2. We assign res
        res = tuple(params_list)

    # 4. We return res
    return res



# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc,
            my_dataset_dir,
            north,
            east,
            south,
            west,
            hours_list
           ):

    # 1. Operation C1: 'textFile'
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: 'map'
    linesRDD = inputRDD.map(process_line)


    # ---------------------------------------
    # TO BE COMPLETED
    # ---------------------------------------

    # Filter 1: Weekend only bus measurements
    # return True : weekday
    # return False: weekend
    def is_date_weekday(entire_csv_row):
      first_date_time_column = entire_csv_row[0]
      date, time = first_date_time_column.split(" ")
      days_of_week = datetime.strptime(date, '%Y-%m-%d').strftime('%A')
      return days_of_week not in ['Saturday', 'Sunday']

    filter_weekday_onlyRDD = linesRDD.filter(lambda x : is_date_weekday(x))

    # Filter 2: Hour in hour_list only bus measurements
    # True: if hour in hours list
    # False: if hour not in hours list
    def is_hour_in_hours_list(entire_csv_row):
      first_date_time_column = entire_csv_row[0]
      date, time = first_date_time_column.split(" ")
      hour = time.split(":")[0]
      return hour in hours_list

    filter_weekday_hour_in_hourslistRDD = filter_weekday_onlyRDD.filter(lambda x : is_hour_in_hours_list(x))

    # Filter 3: Reporting for congestion
    # True: Congestion is 1
    # False: Congestion is 0
    def is_congestion(entire_csv_row):
      congestion = int(entire_csv_row[3])
      return congestion == 1

    filter_weekday_hour_in_hourslist_congestionRDD = filter_weekday_hour_in_hourslistRDD.filter(lambda x : is_congestion(x))

    # Filter 4: is within area of interest
    # True: is within area of interest
    # False: is not within area of interest

    def is_within_area_of_interest(entire_csv_row):
      longitude = entire_csv_row[4]
      latitude = entire_csv_row[5]
      return south <= latitude <= north and west <= longitude <= east

    filter_weekday_hour_in_hourslist_congestion_in_boundsRDD = filter_weekday_hour_in_hourslist_congestionRDD.filter(lambda x : is_within_area_of_interest(x))

    # Group the data by the hour - Ti - Total measurements

    # function to extract the hour
    def get_hour(entire_csv_row):
      first_date_time_column = entire_csv_row[0]
      date, time = first_date_time_column.split(" ")
      hour = time.split(":")[0]
      if hour in hours_list:
        return hour

    def get_hour_interval(entire_csv_row):
      first_date_time_column = entire_csv_row[0]
      date, time = first_date_time_column.split(" ")
      return time

    # everything but congestion:
    filter_weekday_hour_in_hourslistRDD_inbounds = filter_weekday_hour_in_hourslistRDD.filter(lambda x : is_within_area_of_interest(x))

    pairRDD = filter_weekday_hour_in_hourslistRDD_inbounds.map(lambda x : (get_hour_interval(x), 1))

    def get_only_hour(time):
      first_date_time_column = time[0]
      time = first_date_time_column.split(":")[0]
      return time

    # reduce by key for Ti
    # key: hour
    # value: (hour_measurement_count, total congestion) - tuple
    pairpairRDD = pairRDD.map(lambda x : (get_only_hour(x), 1))

    # ti
    # ('09', 3)
    # ('08', 3)
    reduceBykey_Ti_RDD = pairpairRDD.reduceByKey(lambda x, y : x + y)

    #ci
    # ('09', 2)
    # ('08', 1)
    filter_everything_with_congestionRDD = filter_weekday_hour_in_hourslist_congestion_in_boundsRDD.map(lambda x : (get_hour(x), 1))
    reduceBykey_ci_RDD = filter_everything_with_congestionRDD.reduceByKey(lambda x, y : x + y)

    # joing hr, ti and ci
    # ('09', (3, 2))
    # ('08', (3, 1))
    join_hr_ci_ti = reduceBykey_Ti_RDD.join(reduceBykey_ci_RDD)

    solutionRDD_everything = join_hr_ci_ti.map(lambda x : (x[0], round(x[1][1]/x[1][0] * 100, 2) , x[1][0], x[1][1]))

    # sorting - decreassing order of Pi and (tie) - increasing order of Hi
    solutionRDD = solutionRDD_everything.sortBy(lambda x : (-x[1], x[0]))

    # Operation A1: 'collect'
    resVAL = solutionRDD.collect()
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
#i made changes
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

    # 5. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 6. We call to our main function
    my_main(sc,
            my_dataset_dir,
            north,
            east,
            south,
            west,
            hours_list
           )


    print("\n\n\n--- Completed ---")
