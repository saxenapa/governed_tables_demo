import sys
import datetime
import base64
from pyspark.sql import DataFrame, Row
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from awsglue.transforms import ApplyMapping
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'aws_region', 'output_path', 'src_database_name', 'src_table_name', 'dst_database_name', 'dst_table_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 sink locations
aws_region = args['aws_region']
output_path = args['output_path']
src_database_name = args['src_database_name']
src_table_name = args['src_table_name']
dst_database_name = args['dst_database_name']
dst_table_name = args['dst_table_name']

s3_target = output_path + "dem_metrics"
checkpoint_location = output_path + "cp/"
#temp_path = output_path + "temp/"

def transaction_write(context, dfc) -> DynamicFrameCollection:
    dynamic_frame = dfc.select(list(dfc.keys())[0])
    tx_id = context.start_transaction(read_only=False)
    sink = context.getSink(
        connection_type="s3",
        path=s3_target,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
    #    partitionKeys=["city"],
        transactionId=tx_id
    )
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=dst_database_name, catalogTableName=dst_table_name)
    try:
        sink.writeFrame(dynamic_frame)
        dynamic_frame.printSchema()
        context.commit_transaction(tx_id)
    except Exception:
        context.cancel_transaction(tx_id)
        raise
    return dfc

employee_mappings = [
            ("firstname", "string", "firstname", "string"),
            ("lastname", "string", "lastname", "string"),
            ("age", "bigint", "age", "bigint"),
            ("city", "string", "city", "string"),
            ("salary", "bigint", "salary", "bigint")
]
            
def processBatch(data_frame, batchId):
    """Process each batch triggered by Spark Structured Streaming.

    It is called per a batch in order to read DataFrame, apply the mapping, and call TransactionWrite method
    """
    if (data_frame.count() > 0):
        dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
        dynamic_frame_apply_mapping = ApplyMapping.apply(
            frame = dynamic_frame, 
            mappings = employee_mappings,
            transformation_ctx = "dynamic_frame_apply_mapping"
            )
        dynamic_frame.printSchema()
        dynamic_frame_collection = transaction_write(
            glueContext,
            DynamicFrameCollection({"dynamic_frame": dynamic_frame_apply_mapping}, glueContext)
            )
            
# Read from Kinesis Data Stream
sourceData = glueContext.create_data_frame.from_catalog( \
    database = src_database_name, \
    table_name = src_table_name, \
    transformation_ctx = "sourceData", \
    additional_options = {"startingPosition": "TRIM_HORIZON", "inferSchema": "true"})

sourceData.printSchema()

glueContext.forEachBatch(frame = sourceData, batch_function = processBatch, options = {"windowSize": "100 seconds", "checkpointLocation": checkpoint_location})
job.commit()
