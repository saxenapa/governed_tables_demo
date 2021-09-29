import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame

"""
Job Parameters
----------
src_db : string
    Source database in the Glue catalog to read data
src_tbl : string
    Source table in the Glue catalog to read data
target_db : string
    Destination database in the Glue catalog to be loaded
target_tbl : string
    Destination governed table in the Glue catalog to be loaded
dest_path : string
    Destination location to be created and loaded    
"""
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'target_db', 'target_tbl', 'source_path'])

target_database_name = args['target_db']
target_table_name = args['target_tbl']
source_path = args['source_path']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df = spark.read.parquet(source_path)
datasource0 = DynamicFrame.fromDF(df, glueContext, "datasource0")

tx_id = glueContext.start_transaction(False)

sink = glueContext.getSink(connection_type="s3", path=f"s3://lf-data-lake-162611428811/target/{target_table_name}/", enableUpdateCatalog=True, updateBehavior="UPDATE_IN_DATABASE", transactionId=tx_id)
sink.setFormat("glueparquet")
sink.setCatalogInfo(catalogDatabase=target_database_name, catalogTableName=target_table_name)
try:
    sink.writeFrame(datasource0)
    glueContext.commit_transaction(tx_id)
except:
    glueContext.cancel_transaction(tx_id)
    raise
job.commit()
