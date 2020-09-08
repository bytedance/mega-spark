from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml.feature import StringIndexer, VectorAssembler

import os
os.environ["HADOOP_USER_NAME"] = "huangning"
os.environ["PYSPARK_SUBMIT_ARGS"] = '--jars /Users/bytedance/ByteCode/' \
                                    'magellan_megaspark/megaspark/' \
                                    'libs/xgboost4j-0.72.jar,/Users/' \
                                    'bytedance/ByteCode/magellan_megaspark/' \
                                    'megaspark/libs/' \
                                    'xgboost4j-spark-0.72.jar pyspark-shell'

if __name__ == "__main__":

    spark = SparkSession.builder.appName("PySpark XGBOOST Titanic")\
        .master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    spark.sparkContext.addPyFile("/Users/bytedance"
                                 "/ByteCode/magellan_megaspark"
                                 "/megaspark/libs/sparkxgb.zip")
    from sparkxgb import XGBoostEstimator

    # 定义数据结构类型，不让用户自己推断
    schema = StructType(
        [StructField("PassengerId", DoubleType()),
         StructField("Survival", DoubleType()),
         StructField("Pclass", DoubleType()),
         StructField("Name", StringType()),
         StructField("Sex", StringType()),
         StructField("Age", DoubleType()),
         StructField("SibSp", DoubleType()),
         StructField("Parch", DoubleType()),
         StructField("Ticket", StringType()),
         StructField("Fare", DoubleType()),
         StructField("Cabin", StringType()),
         StructField("Embarked", StringType())
         ])

    spark_row = spark \
        .read \
        .schema(schema) \
        .csv("/Users/bytedance"
             "/ByteCode/jupyter/megaspark"
             "/input/titanic/train.csv")
    spark_df = spark_row.na.fill(0)
    spark_df.createGlobalTempView()

    # In order to convert the nominal values into numeric
    # ones we need to define aTransformer for each column:
    sexIndexer = StringIndexer() \
        .setInputCol("Sex") \
        .setOutputCol("SexIndex") \
        .setHandleInvalid("keep")

    cabinIndexer = StringIndexer() \
        .setInputCol("Cabin") \
        .setOutputCol("CabinIndex") \
        .setHandleInvalid("keep")

    embarkedIndexer = StringIndexer() \
        .setInputCol("Embarked") \
        .setOutputCol("EmbarkedIndex") \
        .setHandleInvalid("keep")

    vectorAssembler = VectorAssembler() \
        .setInputCols(["Pclass", "SexIndex", "Age",
                       "SibSp", "Parch", "Fare",
                       "CabinIndex", "EmbarkedIndex"]) \
        .setOutputCol("features")

    xgboost = XGBoostEstimator(
        featuresCol="features",
        labelCol="Survival",
        predictionCol="prediction"
    )

    pipeline = Pipeline().setStages([sexIndexer,
                                     cabinIndexer, embarkedIndexer,
                                     vectorAssembler, xgboost])

    trainDF, testDF = spark_df.randomSplit([0.8, 0.2], seed=24)

    model = pipeline.fit(trainDF)
    model.transform(testDF).select(col("PassengerId"),
                                   col("prediction")).show()
