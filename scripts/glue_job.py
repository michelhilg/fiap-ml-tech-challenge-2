import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1759967833876 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://fiap-fase2-mlet-6/raw"], "recurse": True}, transformation_ctx="AmazonS3_node1759967833876")

# Script generated for node Converter para Data
SqlQuery0 = '''
select
    CAST(data AS date) AS data_ajustada,
    *
from myDataSource
'''
ConverterparaData_node1759972034621 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AmazonS3_node1759967833876}, transformation_ctx = "ConverterparaData_node1759972034621")

# Script generated for node Agrupamento Numérico
AgrupamentoNumrico_node1759972165143 = sparkAggregate(glueContext, parentFrame = ConverterparaData_node1759972034621, groups = ["data_ajustada", "ticker"], aggs = [["Low", "min"], ["High", "max"], ["Volume", "sum"]], transformation_ctx = "AgrupamentoNumrico_node1759972165143")

# Script generated for node Renomeando Colunas
RenomeandoColunas_node1760004597527 = ApplyMapping.apply(frame=AgrupamentoNumrico_node1759972165143, mappings=[("data_ajustada", "date", "data", "date"), ("ticker", "string", "ticker", "string"), ("`min(Low)`", "double", "preco_min", "double"), ("`max(High)`", "double", "preco_max", "double"), ("`sum(Volume)`", "long", "volume_negociado", "long")], transformation_ctx="RenomeandoColunas_node1760004597527")

# Script generated for node Cálculo com Base na Data
SqlQuery1 = '''
-- Calcula média móvel de preço
SELECT
    data,
    ticker,
    preco_min,
    preco_max,
    volume_negociado,
    AVG(preco_min) OVER (
        PARTITION BY ticker 
        ORDER BY data 
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) AS media_movel_3d_minima,
    AVG(preco_max) OVER (
        PARTITION BY ticker 
        ORDER BY data
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) AS media_movel_3d_maxima
FROM
    myDataSource
ORDER BY
    ticker, data
'''
ClculocomBasenaData_node1760004654385 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"myDataSource":RenomeandoColunas_node1760004597527}, transformation_ctx = "ClculocomBasenaData_node1760004654385")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ClculocomBasenaData_node1760004654385, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760006250821", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1760006314488 = glueContext.write_dynamic_frame.from_options(frame=ClculocomBasenaData_node1760004654385, connection_type="s3", format="glueparquet", connection_options={"path": "s3://fiap-fase2-mlet-6/refined/", "partitionKeys": ["data", "ticker"]}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1760006314488")

job.commit()