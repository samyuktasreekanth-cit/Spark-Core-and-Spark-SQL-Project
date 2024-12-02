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
            vehicle_id,
            day_picked,
            delay_limit
           ):

    # 1. Operation C1: 'textFile'
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: 'map'
    linesRDD = inputRDD.map(process_line)

    # ---------------------------------------
    # TO BE COMPLETED
    # ---------------------------------------

    # Filter 1: vehicleID
    # True: if vehicleID is concrete ID (vehicle_id)
    # False: wrong Vehicle ID
    def is_vehicleID(entire_csv_row):
      vehicle_ID_from_column = entire_csv_row[7]
      return vehicle_id == vehicle_ID_from_column

    filter_vehicleID_RDD = linesRDD.filter(lambda x : is_vehicleID(x))

    # Filter 2: Day
    # True: if selected date matches the concrete day (day_picked)
    # False: wrong day
    def is_correct_day(entire_csv_row):
      first_date_time_column = entire_csv_row[0]
      date, time = first_date_time_column.split(" ")
      return day_picked == date

    filter_vehicleID_day_RDD = filter_vehicleID_RDD.filter(lambda x : is_correct_day(x))

    # Filter 3: does stop at station
    # True: if stops at station (At_stop_stop = 1)
    # False:  if does not stop at station (At_stop_stop = 0)
    def does_stop_station(entire_csv_row):
      at_stop_stop_column = entire_csv_row[9]
      return at_stop_stop_column == 1

    filter_vehicleID_day_stopsAtStation_RDD = filter_vehicleID_day_RDD.filter(lambda x : does_stop_station(x))

    def get_hour(entire_csv_row):
      first_date_time_column = entire_csv_row[0]
      date, time = first_date_time_column.split(" ")
      return time

    # Sort the data
    sort_date_time = filter_vehicleID_day_stopsAtStation_RDD.sortBy(lambda x: get_hour(x))

    # Track the arrival times of the the bus VehicleID at each Closest_Stop(Bus_Station_ID) for each Bus_Line
    # Group by ((bus_line, bus_stop), (entire_csv_row))
    groupBy_busLine_busStop_RDD = sort_date_time.groupBy(lambda x : (x[1], x[8])).mapValues(lambda x: list(sorted(x)))

    # keeping only the first index from each group. as the index 2 are conitnuations from index 1
    groupBy_busLine_busStop_first_index_RDD = groupBy_busLine_busStop_RDD.mapValues(lambda x : x[0])

    # sort the group by
    sorted_group_by = groupBy_busLine_busStop_first_index_RDD.sortBy(lambda x : get_hour(x[1]))

    # Sticking to the delay limit - check if it is on schedule
    # remove anything above or below the delay_limit
    def is_on_schedule(entire_csv_row):
        delay = entire_csv_row[6]
        # given formula
        is_within_limit = ((-1.0) * delay_limit) <= delay <= delay_limit
        if is_within_limit:
            return 1
        else:
            return 0

    # map for the timetable
    #(bus_line, stationID(closest_stop))
    solutionRDD = sorted_group_by.map(lambda x : (x[1][1], x[1][8], x[1][0].split()[1], is_on_schedule(x[1])))

    # ---------------------------------------

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

    # 5. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 6. We call to our main function
    my_main(sc,
            my_dataset_dir,
            vehicle_id,
            day_picked,
            delay_limit
           )

    print("\n\n\n--- Completed ---")
