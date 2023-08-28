# Databricks notebook source
from pyspark.sql.functions import col,explode_outer,when

# COMMAND ----------

dbutils.fs.mount(
    source="wasbs://databrickscontainer@assignmentstorage1111.blob.core.windows.net/",
    mount_point="/mnt/mountfiles",
    extra_configs={"fs.azure.account.key.assignmentstorage1111.blob.core.windows.net":"/D6I3OAw+QLgb4lw+COcVAeZZy+FrzdoZikFiO8ZdObK52xWhnvUvvk4/U91Skg6uVaYkQLaU56W+AStwHqYSQ=="}
)

# COMMAND ----------

# Reading and displaying CSV data
csv_df = spark.read.csv('/mnt/mountfiles/sample_csv.csv',header = True)
display(csv_df)

# COMMAND ----------

#Reading and Displaying Json Data
json_df = spark.read.option('multiline','true').json('/mnt/mountfiles/sample_json_1.json')
display(json_df)

# COMMAND ----------

# Converting Json data to Parquet and writing to Bronze layer 
parquet_output_json = '/mnt/mountfiles/bronze/json'
json_df.write.parquet(parquet_output_json)

# COMMAND ----------

# Converting csv data to Parquet and writing to Bronze layer
parquet_output_csv = '/mnt/mountfiles/bronze/csv'
csv_df.write.parquet(parquet_output_csv)

# COMMAND ----------

# Displaying both parquet files 
parquet_df_csv = spark.read.parquet(parquet_output_csv)
display(parquet_df_csv)
parquet_df_csv.printSchema()

parquet_df_json = spark.read.parquet(parquet_output_json)
display(parquet_df_json)
parquet_df_json .printSchema()

# COMMAND ----------

# flattenning the json_parquet 
flatten_df_json = parquet_df_json.withColumn('name',explode_outer('projects.name'))\
                            .withColumn('status',explode_outer('projects.status'))\
                            .drop('projects')
display(flatten_df_json)
flatten_df.printSchema()

# COMMAND ----------

# Removing the duplicates from both dataFrames
distinct_df_csv = parquet_df_csv.dropDuplicates()
display(distinct_df_csv)

distinct_df_json = flatten_df_json.dropDuplicates()
display(distinct_df_json)

# COMMAND ----------

# Filling null as 0 in csv_dataframe
fill_nulls_csv = distinct_df_csv.fillna('0')
fill_nulls_csv= fill_nulls.withColumn('city',when(col('city') == 'Null','0').otherwise(col('city')))
display(fill_nulls_csv)
fill_nulls_csv.printSchema()

# COMMAND ----------

# Filling null as 0 in json_dataframe
fill_nulls_json = distinct_df_json.fillna('0')
fill_nulls_json = distinct_df_json.na.fill({'id': 0,'salary': 0,'name': 0,'status':0,'department':0})
display(fill_nulls_json)
fill_nulls_json.printSchema()

# COMMAND ----------

colupper_df_csv = fill_nulls_csv.toDF(*[col.title() for col in fill_nulls_csv.columns])
display(colupper_df_csv)
colupper_df_json = fill_nulls_json.toDF(*[col.title() for col in fill_nulls_json.columns])
display(colupper_df_json)

# COMMAND ----------

# Writting to silver layer 
silver_csv = colupper_df_csv.write \
    .format("parquet") \
    .mode("overwrite")\
    .save('/mnt/mountfiles/silver/csv')
    
silver_json = colupper_df_json.write \
    .format("parquet") \
    .mode("overwrite")\
    .save('/mnt/mountfiles/silver/json')



# COMMAND ----------

#Displaying the tables in silver layer
silver_csv1 = spark.read.parquet('/mnt/mountfiles/silver/csv')
display(silver_csv1)

silver_json1 = spark.read.parquet('/mnt/mountfiles/silver/json')
display(silver_json1)

# COMMAND ----------

silver_json1 = silver_json1.withColumnRenamed('NameOfProject','ProjectName')
display(silver_json1)

# COMMAND ----------

# Joining both the dataframes 
join_dataframe = silver_csv1.join(silver_json1, on='Id', how="outer")
display(join_dataframe)

        

# COMMAND ----------

#Writting to Gold layer 
gold_layer = '/mnt/mountfiles/gold'
join_dataframe.write.parquet(gold_layer)

# COMMAND ----------

joined_df = spark.read.parquet(gold_layer)
display(joined_df)


# COMMAND ----------

ids_to_delete = [31, 40, 7, 15]
filtered_df = joined_df.filter(~joined_df["Id"].isin(ids_to_delete))
display(filtered_df)
