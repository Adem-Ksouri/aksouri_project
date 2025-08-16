# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

def readScanningData():
    
    df = (
        spark.read.format('json').load('/Volumes/{catalog}/bronze/bronze_json_files/*.json').withColumn('Id', F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))
    )

    return df

# COMMAND ----------

def filter_Data(df, column):

    df_filter = df.filter(F.size(F.col(column)) > 0)

    return df_filter

# COMMAND ----------

def data_extraction_step(df, attributes):
    schema = StructType([
        StructField("StepTiming", MapType(StringType(), StringType())),  # or nested struct
        StructField("DataExtractionEngine", IntegerType()),
        StructField("DataExtractionEngineStr", StringType()),
        StructField("LLMModel", StringType()),
        StructField("SerializedExtractionResult", StringType()),
        StructField("Succeeded", BooleanType())
    ])
    
    df.select('Id', F.col('DataExtractionStepMetaData'))

    df_exploded = df.withColumn('DataExtractionStep', F.explode('DataExtractionStepMetaData'))

    df_exploded.drop(F.col('DataExtractionStepMetaData'))

    df_parsed = df_exploded.withColumn("jsonData", F.from_json(F.col("DataExtractionStep"), schema))

    df_flat = df_parsed.select(
        "id",
        F.col("jsonData.DataExtractionEngine").alias("DataExtractionEngine"),
        F.col("jsonData.DataExtractionEngineStr").alias("DataExtractionEngineStr"),
        F.col("jsonData.LLMModel").alias("LLMModel"),
        F.col("jsonData.SerializedExtractionResult").alias("SerializedExtractionResult"),
        F.col("jsonData.Succeeded").alias("Succeeded"),
        F.col("jsonData.StepTiming").alias("StepTiming")  # still nested if needed
    )

    return df_flat

# COMMAND ----------


