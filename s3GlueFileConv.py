#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python
# coding: utf-8

from pyspark.context import SparkContext
from pyspark.sql.functions import * 
import boto3
import pandas as pd
import io
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

#Initialize contexts and session
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

#Parameters
glue_db = "dvddlcatalog"
glue_tbl = "dvdactor"
s3_write_path = "s3://parqdatalake/dvdactor"

#Read movie data to Glue dynamic frame
dynamic_frame_read = glue_context.create_dynamic_frame.from_catalog(database = glue_db, table_name = glue_tbl)

#Convert dynamic frame to data frame to use standard pyspark functions
df = dynamic_frame_read.toDF()

#Convert back to dynamic frame
dynamic_frame_write = DynamicFrame.fromDF(df, glue_context, "dynamic_frame_write")

#Write data back to S3
glue_context.write_dynamic_frame.from_options(
    frame = dynamic_frame_write,
    connection_type = "s3",
    connection_options = {
        "path": s3_write_path,
        #Here you could create S3 prefixes according to a values in specified columns
        #"partitionKeys": ["actor_id"]
    },
    format = "parquet"
)

