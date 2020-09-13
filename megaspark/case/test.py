import megaspark.tomega as tm

from megaspark.ml.mega_xgboost import XGBoostClassifier
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql.types import ByteType, ShortType, \
    IntegerType, LongType, FloatType, DoubleType, BooleanType, StringType
from pyspark.sql.types import StructType, StructField

import os
# os.environ["HADOOP_USER_NAME"] = "huangning"
os.environ["PYSPARK_SUBMIT_ARGS"] = '--jars ./megaspark/' \
                                    'libs/xgboost4j-0.72.jar,./megaspark/libs/' \
                                    'xgboost4j-spark-0.72.jar'

if __name__ == "__main__":

    input_path = "/Users/bytedance/ByteCode/magellan_megaspark/data/cs-training.csv"
    data_df = tm.read_csv(input_path, header=False, feats_info={"PassengerId": "double",
                                                               "Survived": "double",
                                                               "Pclass": "double",
                                                               "Name": "string",
                                                               "Sex": "string",
                                                               "Age": "double",
                                                               "SibSp": "double",
                                                               "Parch": "double",
                                                               "Ticket": "string",
                                                               "Fare": "double",
                                                               "Cabin": "string",
                                                               "Embarked": "string"})

    data_df = data_df.na.fill(0)
    data_df.mega.table_alias("student")
    sample_df = tm.sql("select * from student limit 10")
    df = sample_df.mega.fillna({"Survived": 0, "Cabin": "unknown"})

    # 将数据集分割成训练集和验证集
    trainDF, testDF = tm.train_test_split(data_df, test_size=0.9, random_state=99)

    # model train
    xgb_clf = XGBoostClassifier("features", "Survived", "prediction")
    sample_df.show(3)
    xgb_clf.fit(sample_df)
    res = xgb_clf.predict_proba(sample_df)
    print(res.mega.head(5))

    # spark = SparkSession.builder.appName("mega sql").master("local[*]").getOrCreate()
    # spark.sparkContext.addPyFile("/Users/bytedance"
    #                              "/ByteCode/magellan_megaspark"
    #                              "/megaspark/libs/sparkxgb.zip")
    # from sparkxgb import XGBoostEstimator

    #     schema = StructType(
    #         [StructField("PassengerId", DoubleType()),
    #          StructField("Survived", DoubleType()),
    #          StructField("Pclass", DoubleType()),
    #          StructField("Name", StringType()),
    #          StructField("Sex", StringType()),
    #          StructField("Age", DoubleType()),
    #          StructField("SibSp", DoubleType()),
    #          StructField("Parch", DoubleType()),
    #          StructField("Ticket", StringType()),
    #          StructField("Fare", DoubleType()),
    #          StructField("Cabin", StringType()),
    #          StructField("Embarked", StringType())
    #          ])
    # data_df = spark.read.option("header", "true").schema(schema).csv(input_path)
    # spark_df = data_df.na.fill(0)

    # data_df = spark.read.option("header", "true").csv(input_path)

    # # In order to convert the nominal values into numeric
    # # ones we need to define aTransformer for each column:
    # sexIndexer = StringIndexer() \
    #     .setInputCol("Sex") \
    #     .setOutputCol("SexIndex") \
    #     .setHandleInvalid("keep")
    #
    # cabinIndexer = StringIndexer() \
    #     .setInputCol("Cabin") \
    #     .setOutputCol("CabinIndex") \
    #     .setHandleInvalid("keep")
    #
    # embarkedIndexer = StringIndexer() \
    #     .setInputCol("Embarked") \
    #     .setOutputCol("EmbarkedIndex") \
    #     .setHandleInvalid("keep")
    #
    # vectorAssembler = VectorAssembler() \
    #     .setInputCols(["Pclass", "SexIndex", "Age",
    #                    "SibSp", "Parch", "Fare",
    #                    "CabinIndex", "EmbarkedIndex"]) \
    #     .setOutputCol("features")

    # # from megaspark.ml.sparkxgb import XGBoostEstimator
    # xgboost = XGBoostEstimator(
    #     featuresCol="features",
    #     labelCol="SeriousDlqin2yrs",
    #     predictionCol="prediction"
    # )
    # pipeline = Pipeline().setStages([sexIndexer,
    #                                  cabinIndexer, embarkedIndexer,
    #                                  vectorAssembler, xgboost])
    # trainDF, testDF = spark_df.randomSplit([0.6, 0.4], seed=24)
    # trainDF.show(5)
    #
    # model = pipeline.fit(testDF)
    # model.transform(testDF).select(col("PassengerId"), col("prediction")).show()
