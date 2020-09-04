# 通过初始化找到本机安装的spark的环境
import findspark

findspark.init()

from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession \
        .builder\
        .appName("test_xgboost") \
        .master("local") \
        .getOrCreate()

    # .config("spark.jars.packages", "/Users/bytedance/ByteCode/magellan_s2p/libs/xgboost4j-0.90.jar,
    # /Users/bytedance/ByteCode/magellan_s2p/libs/xgboost4j-spark-0.90.jar") \
    # 将多个jar包添加到spark环境，中间用逗号隔开

    spark_df = spark.read.option("header", "true").csv("./input/train.csv")
    spark_df.show(5)
