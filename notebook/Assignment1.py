# Databricks notebook source
from pyspark.sql.functions import col,explode_outer,when

# COMMAND ----------

try:    
    dbutils.fs.mount(
        source="wasbs://databrickscontainer@assignmentstorage1111.blob.core.windows.net/",
        mount_point="/mnt/mountfiles",
        extra_configs={"fs.azure.account.key.assignmentstorage1111.blob.core.windows.net":"/D6I3OAw+QLgb4lw+COcVAeZZy+FrzdoZikFiO8ZdObK52xWhnvUvvk4/U91Skg6uVaYkQLaU56W+AStwHqYSQ=="}
    )
except:
    print("Already Mounted")   

# COMMAND ----------

# DBTITLE 1, Reading and displaying CSV data
def display_csvdata():    
    csv_df = spark.read.csv('/mnt/mountfiles/sample_csv.csv',header = True)
    display(csv_df)
    return csv_df
   

# COMMAND ----------

csv_dataframe = display_csvdata()

# COMMAND ----------

# DBTITLE 1,Reading and Displaying Json Data
 def display_jsondata():   
    json_df = spark.read.option('multiline','true').json('/mnt/mountfiles/sample_json_1.json')
    display(json_df)
    return json_df

# COMMAND ----------

json_data_frame = display_jsondata()

# COMMAND ----------

# DBTITLE 1,Converting Json data to Parquet and writing to Bronze layer 
def bronze_jsondata(json_df):    
    parquet_output_json = '/mnt/mountfiles/bronze/json'
    json_df.write.parquet(parquet_output_json)
    return parquet_output_json
bronze_jsondata(json_data_frame)    

# COMMAND ----------

json_bronze = bronze_jsondata(json_data_frame)

# COMMAND ----------

# DBTITLE 1,Converting csv data to Parquet and writing to Bronze layer
def bronze_csvdata(csv_df):    
    parquet_output_csv = '/mnt/mountfiles/bronze/csv'
    csv_df.write.parquet(parquet_output_csv)
    return parquet_output_csv

bronze_csvdata(csv_dataframe)

# COMMAND ----------

parquet_csv_output = bronze_csvdata(csv_dataframe)

# COMMAND ----------

# DBTITLE 1,Displaying both parquet files 
def display_parquetdata_csv(parquet_output_csv):    
    parquet_df_csv = spark.read.parquet(parquet_output_csv)
    display(parquet_df_csv)
    parquet_df_csv.printSchema()
    return parquet_df_csv


# COMMAND ----------

display_csv1 = display_parquetdata_csv(parquet_csv_output)

# COMMAND ----------

def display_parquetdata_json(parquet_output_json):     
    parquet_df_json = spark.read.parquet(parquet_output_json)
    display(parquet_df_json)
    parquet_df_json .printSchema()
    return parquet_df_json

# COMMAND ----------

# DBTITLE 1,flattenning the json_parquet 
def flatten_jsondata(parquet_df_json):
    flatten_df_json = parquet_df_json.withColumn('name',explode_outer('projects.name'))\
                                .withColumn('status',explode_outer('projects.status'))\
                                .drop('projects')
    display(flatten_df_json)
    flatten_df.printSchema()
    return flatten_df_json

# COMMAND ----------

# DBTITLE 1,Removing the duplicates from both dataFrames
def remove_duplicates_csv(parquet_df_csv):
    distinct_df_csv = parquet_df_csv.dropDuplicates()
    display(distinct_df_csv)
    return distinct_df_csv


# COMMAND ----------

def remove_duplicates_json(parquet_df_json):
    distinct_df_json = flatten_df_json.dropDuplicates()
    display(distinct_df_json)
    return distinct_df_json

# COMMAND ----------

# DBTITLE 1,Filling null as 0 in csv_dataframe
def fill_nulls(distinct_df_csv):
    fill_nulls_csv = distinct_df_csv.fillna('0')
    fill_nulls_csv= fill_nulls_csv.withColumn('city',when(col('city') == 'Null','0').otherwise(col('city')))
    display(fill_nulls_csv)
    fill_nulls_csv.printSchema()
    return fill_nulls_csv

# COMMAND ----------

# DBTITLE 1,Filling null as 0 in json_dataframe
def fill_nulljson(distinct_df_json):
    fill_nulls_json = distinct_df_json.fillna('0')
    fill_nulls_json = distinct_df_json.na.fill({'id': 0,'salary': 0,'name': 0,'status':0,'department':0})
    display(fill_nulls_json)
    fill_nulls_json.printSchema()
    return fill_nulljson

# COMMAND ----------

 def title_case_csv(fill_nulls_csv):   
    colupper_df_csv = fill_nulls_csv.toDF(*[col.title() for col in fill_nulls_csv.columns])
    display(colupper_df_csv)
    return colupper_df_csv


# COMMAND ----------

 def title_case_json(fill_nulls_json):
    colupper_df_json = fill_nulls_json.toDF(*[col.title() for col in fill_nulls_json.columns])
    display(colupper_df_json)
    return colupper_df_json

# COMMAND ----------

# DBTITLE 1,Writting to silver layer 
def csv_silver(colupper_df_csv):
    silver_csv = colupper_df_csv.write \
        .format("parquet") \
        .mode("overwrite")\
        .save('/mnt/mountfiles/silver/csv')
    return silver_csv  

# COMMAND ----------

def json_silver(colupper_df_json)    
    silver_json = colupper_df_json.write \
        .format("parquet") \
        .mode("overwrite")\
        .save('/mnt/mountfiles/silver/json')
    return silver_json    

# COMMAND ----------

 def display_silver_csv():   
    #Displaying the tables in silver layer
    silver_csv1 = spark.read.parquet('/mnt/mountfiles/silver/csv')
    display(silver_csv1)
    return silver_csv1


# COMMAND ----------

def display_silver_json():
    silver_json1 = spark.read.parquet('/mnt/mountfiles/silver/json')
    display(silver_json1)
    return silver_json1

# COMMAND ----------

def rename_column(silver_json1):    
    silver_json1 = silver_json1.withColumnRenamed('NameOfProject','ProjectName')
    display(silver_json1)
    return silver_json1

# COMMAND ----------

# DBTITLE 1,Joining both the dataframes 
def join_df(silver_csv1):    
    join_dataframe = silver_csv1.join(silver_json1, on='Id', how="outer")
    display(join_dataframe)  
    return join_dataframe 

# COMMAND ----------

# DBTITLE 1,Writting to Gold layer 
def writegold_layer():    
    gold_layer = '/mnt/mountfiles/gold'
    join_dataframe.write.parquet(gold_layer)
    return gold_layer

# COMMAND ----------

def display_gold(gold_layer):    
    joined_df = spark.read.parquet(gold_layer)
    display(joined_df)
    return joined_df


# COMMAND ----------

def filter_columns(oined_df):    
    ids_to_delete = [31, 40, 7, 15]
    filtered_df = joined_df.filter(~joined_df["Id"].isin(ids_to_delete))
    display(filtered_df)
    return filtered_df

# COMMAND ----------

display_csvdata()
