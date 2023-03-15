import configparser
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.functions import udf, col, lit, year, month, upper, to_date
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import udf, lit, explode, split, regexp_extract, col, isnan, isnull, desc, when, sum, to_date, desc, regexp_replace, count, to_timestamp
from pyspark.sql.types import IntegerType, TimestampType



#global variables
input_data =''
output_data = './output/'

@udf(TimestampType())
def fix_date(x):
    try:
        return pd.to_timedelta(x, unit='D') + pd.Timestamp('1960-1-1')
    except:
        return pd.Timestamp('1900-1-1') 
    



def process_fact_table(spark) -> None:
    
   
    # read immigration data file
    immi_data = os.path.join(input_data + '../../data/18-83510-I94-Data-2016/*.sas7bdat')
    df = spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    print('df loaded')

    # extract columns to create fact_immigration table
    df = df.withColumn('dtadfile', to_date(col('dtadfile'), format='yyyyMMdd'))\
               .withColumn('dtadddto', to_date(col('dtaddto'), format='MMddyyyy'))
    
    df = df.withColumn('arrdate', to_date(fix_date(col('arrdate'))))\
               .withColumn('depdate', to_date(fix_date(col('depdate'))))
    
    fact_immigration = df.select('cicid', 'i94yr', 'i94mon', 'i94port', 'i94addr',\
                                 'arrdate', 'depdate', 'i94mode', 'i94visa').distinct()\
                         .withColumn("immigration_id", monotonically_increasing_id())
    


    fact_immigration = fact_immigration.withColumn('country', lit('United States'))


    fact_immigration.write.mode("overwrite").partitionBy('i94addr')\
                    .parquet(path=output_data + 'fact_immigration')
    print('fact table complete')
    
    
def process_personal_data (spark) -> None:
    
    # read immigration data file
    immi_data = os.path.join(input_data + '../../data/18-83510-I94-Data-2016/*.sas7bdat')
    df = spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    print('df loaded')

    df = df.withColumn('dtadfile', to_date(col('dtadfile'), format='yyyyMMdd'))\
               .withColumn('dtadddto', to_date(col('dtaddto'), format='MMddyyyy'))
    
    df = df.withColumn('arrdate', to_date(fix_date(col('arrdate'))))\
               .withColumn('depdate', to_date(fix_date(col('depdate'))))
    

    personal = df.select('cicid', 'i94cit', 'i94res',\
                                  'biryear', 'gender', 'i94visa').distinct()\
                          .withColumn("personal_id", monotonically_increasing_id())
    
   

    personal.write.mode("overwrite")\
                     .parquet(path=output_data + 'personal')
    print('personal table saved')
    
    #airline dimension table processing

    airline = df.select('cicid', 'airline', 'admnum', 'fltno').distinct().withColumn("immi_airline_id", monotonically_increasing_id())
   

    airline.write.mode("overwrite").parquet(path=output_data + 'airline')
    
    #visa dimension table processing
    vis_dim=df.select(["i94visa","visapost"])\
                       .withColumnRenamed("i94visa","Visa_id")
    
    vis_dim.write.mode("overwrite").parquet(path=output_data + 'vis_dim')
    print('visa table complete')


def process_temperature_data (spark) -> None:

    tempe_data = os.path.join(input_data + '../../data2/GlobalLandTemperaturesByCity.csv')
    df = spark.read.csv(tempe_data, header=True)
    print('temp df loaded')

    df = df.where(df['Country'] == 'United States')
    dim_temperature = df.select(['dt', 'AverageTemperature', 'AverageTemperatureUncertainty',\
                         'City', 'Country']).distinct()


    dim_temperature = dim_temperature.withColumn('dt', to_date(col('dt')))
    dim_temperature = dim_temperature.withColumn('year', year(dim_temperature['dt']))
    dim_temperature = dim_temperature.withColumn('month', month(dim_temperature['dt']))
 
    # write dim_temperature table to parquet files
    dim_temperature.write.mode("overwrite")\
                   .parquet(path=output_data + 'dim_temperature')
    print('temp table complete')



def process_demography_data(spark) -> None: 


    demog_data = os.path.join(input_data + 'us-cities-demographics.csv')
    df = spark.read.csv(demog_data, header=True)
    print ('demog df loaded')


    demographics = df.select(['City', 'State', 'Male_Population', 'Female_Population', \
                              'No_of_Veterans', 'Race', 'Foreign_born', 'Avg_Household_Size']).distinct() \
                              .withColumn("demog_pop_id", monotonically_increasing_id())

    demographics.write.mode("overwrite")\
                        .parquet(path=output_data + 'demographics')
    print('demog table complete')

    
def main():

    spark = SparkSession.builder\
        .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .config('spark.debug.maxToStringFields', 2000)\
        .enableHiveSupport().getOrCreate()
    #spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

    process_fact_table(spark)  
    process_personal_data (spark)
    process_temperature_data(spark)
    process_demography_data(spark)
    


if __name__ == "__main__":
    main()
