from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from automatedpbttruepython.config.ConfigStore import *
from automatedpbttruepython.udfs.UDFs import *
from prophecy.utils import *
from automatedpbttruepython.graph import *

def pipeline(spark: SparkSession) -> None:
    df_s3_source_dataset = s3_source_dataset(spark)
    Lookup_1(spark, df_s3_source_dataset)
    df_SQLStatement_1 = SQLStatement_1(spark, df_s3_source_dataset)
    df_Reformat_1 = Reformat_1(spark, df_s3_source_dataset)
    df_Script_1 = Script_1(spark, df_Reformat_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/AutomatedPBT-truepython")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/AutomatedPBT-truepython", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/AutomatedPBT-truepython")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
