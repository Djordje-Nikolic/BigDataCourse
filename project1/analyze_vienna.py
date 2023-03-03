import os.path as path
import pyspark
import pyspark.sql.functions as sparkFun
import sys
import time

def writeDf(df, path):
    df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", True) \
        .format("csv") \
        .save(path)

def analyzeByFilter(df, x_min, x_max, y_min, y_max, timestep_min, timestep_max):
    filteredDf = df.filter((df.timestep >= timestep_min) & (df.timestep <= timestep_max) & (df.x >= x_min) & (df.x <= x_max) & (df.y >= y_min) & (df.y <= y_max)) \
        .withColumn('temp', sparkFun.lit(1))

    res = filteredDf \
        .groupBy("temp") \
        .agg((sparkFun.min('speed'), 
            sparkFun.avg('speed'), 
            sparkFun.max('speed'), 
            sparkFun.stddev('speed'), 
            sparkFun.min('acceleration'), 
            sparkFun.avg('acceleration'), 
            sparkFun.max('acceleration'), 
            sparkFun.stddev('acceleration'), 
            sparkFun.min('odometer'), 
            sparkFun.avg('odometer'), 
            sparkFun.max('odometer'), 
            sparkFun.stddev('odometer'), 
            sparkFun.count_distinct('id').alias('unique_vehicles')))

    res2 = filteredDf \
        .groupBy("temp", "type") \
        .agg(sparkFun.count_distinct("id")) \
        .groupBy("temp") \
        .pivot("type") \
        .sum("count(id)")

    final = res.join(res2, ["temp"]).drop('temp')

    return final

def analyzeByLaneAndHour(df):
    res = df.withColumn("hour", sparkFun.floor(df['timestep'] / 3600)) \
        .groupBy("lane", "hour") \
        .agg((sparkFun.min('speed'), 
            sparkFun.avg('speed'), 
            sparkFun.max('speed'), 
            sparkFun.stddev('speed'), 
            sparkFun.min('acceleration'), 
            sparkFun.avg('acceleration'), 
            sparkFun.max('acceleration'), 
            sparkFun.stddev('acceleration'), 
            sparkFun.min('odometer'), 
            sparkFun.avg('odometer'), 
            sparkFun.max('odometer'), 
            sparkFun.stddev('odometer'), 
            sparkFun.count_distinct('id').alias('unique_vehicles')))

    res2 = df.withColumn("hour", sparkFun.floor(df['timestep'] / 3600)) \
        .groupBy("lane", "hour", "type") \
        .agg(sparkFun.count_distinct("id")) \
        .groupBy("lane", "hour") \
        .pivot("type") \
        .sum("count(id)")

    final = res.join(res2, ["lane", "hour"]).orderBy(["lane", "hour"])

    return final

if __name__ == "__main__":
    if len(sys.argv) < 9:
        raise Exception("Missing command line argument for path.")

    dataSetFilePath = sys.argv[1]
    outputPath = sys.argv[2]
    x_min = sys.argv[3]
    x_max = sys.argv[4]
    y_min = sys.argv[5]
    y_max = sys.argv[6]
    time_min = sys.argv[7]
    time_max = sys.argv[8]

    start = time.time()
    spark = pyspark.sql.SparkSession \
        .builder \
        .appName("Python Spark SQL Vienna Analysis") \
        .getOrCreate()

    df = spark.read \
        .option('header', True) \
        .option('inferSchema', True) \
        .csv(dataSetFilePath)

    resultAll = analyzeByLaneAndHour(df)
    writeDf(resultAll, path.join(outputPath, "all_results"))

    resultFilter = analyzeByFilter(df, x_min=x_min, x_max=x_max, y_min=y_min, y_max=y_max, timestep_min=time_min, timestep_max=time_max)  
    writeDf(resultFilter, path.join(outputPath, "filter_results"))

    spark.stop()

    end = time.time()
    print(f"The amount of time execute this job: {end-start}s")