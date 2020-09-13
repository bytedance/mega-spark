import findspark
findspark.init("/usr/local/Cellar/spark-2.4.6-bin-hadoop2.7")

import os
os.environ["HADOOP_USER_NAME"] = "huangning"
os.environ["PYSPARK_SUBMIT_ARGS"] = '--jars /Users/bytedance/ByteCode/' \
                                    'magellan_megaspark/megaspark/' \
                                    'libs/xgboost4j-0.72.jar,/Users/' \
                                    'bytedance/ByteCode/magellan_megaspark/' \
                                    'megaspark/libs/' \
                                    'xgboost4j-spark-0.72.jar pyspark-shell'

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from megaspark.sql.megaframe import MegaFrame
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import ByteType, ShortType, \
    IntegerType, LongType, FloatType, DoubleType, BooleanType, StringType


spark = SparkSession\
    .builder\
    .appName("mega sql")\
    .master("local")\
    .getOrCreate()
spark.sparkContext.addPyFile("/Users/bytedance"
                             "/ByteCode/magellan_megaspark"
                             "/megaspark/libs/sparkxgb.zip")
from sparkxgb import XGBoostEstimator


print("Welcome Mega Spark ～")


def show_func():

    print("+-------------------+")
    print("|sql method         |")
    print("+-------------------+")
    print("|1. read_csv        |")
    print("|2. sql             |")
    print("+-------------------+")
    print("|MegaFrame method   |")
    print("+-------------------+")
    print("|1. head            |")
    print("|2. table_alias     |")
    print("|3. sort_values     |")
    print("|4. fillna          |")
    print("+-------------------+")


def read_csv(input_path, header=True, feats_info={}):
    """Read a comma-separated values (csv) file into spark DataFrame.

    Parameters
    ----------
    input_path : str
        Any valid string path is acceptable.
        The string should be the path of HDFS.

    header : bool, default=True
        If True, the real column names of the
        data frame will be shown. if False, the
        column name of the DataFrame will be
        displayed as _c0, _c1, _c2 and so on.

    feat_info : dict, default={}
        Mapping of features and feature types

    Returns
    --------
    spark_df : DataFrame
        The result of the read csv file.


    Examples
    ----------
    >>> input_path = "/path/to/sample.csv"
    >>> data_df =  mg.read_csv(input_path, True)
    >>> data_df.mega.head(3)
      PassengerId Survived Pclass  ...     Fare Cabin Embarked
    0           1        0      3  ...     7.25  None        S
    1           2        1      1  ...  71.2833   C85        C
    2           3        1      3  ...    7.925  None        S
    3           4        1      1  ...     53.1  C123        S
    4           5        0      3  ...     8.05  None        S
    """
    if len(feats_info) == 0:
        return spark.read.option("header", header).csv(input_path)

    schema = []
    for feat_name, feat_type in feats_info.items():

        if feat_type == "bool":
            schema.append(StructField(feat_name, DoubleType()))
        elif feat_type == "string":
            schema.append(StructField(feat_name, StringType()))
        elif feat_type == "byte":
            schema.append(StructField(feat_name, ByteType()))
        elif feat_type == "short":
            schema.append(StructField(feat_name, ShortType()))
        elif feat_type == "int":
            schema.append(StructField(feat_name, IntegerType()))
        elif feat_type == "long":
            schema.append(StructField(feat_name, LongType()))
        elif feat_type == "float":
            schema.append(StructField(feat_name, FloatType()))
        elif feat_type == "double":
            schema.append(StructField(feat_name, DoubleType()))
        else:
            raise Exception("Current type can not handle")

    schema_info = StructType(schema)
    return spark.read.option("header", header).schema(schema_info).csv(input_path)


def sql(sql_query):
    """Read a comma-separated values (csv) file into spark DataFrame.

    Parameters
    ----------
    sqlquery : str
        Query statement


    Returns
    --------
    spark_df : DataFrame
        The result of the given query.


    Examples
    ----------
    >>> df.mega.table_alias("people")
    >>> data_df =  mg.sql("SELECT field1 AS f1, field2 AS f2 from people")
    >>> data_df.mega.head(5)
          PassengerId Survived Pclass  ...     Fare Cabin Embarked
    0           1        0      3  ...     7.25  None        S
    1           2        1      1  ...  71.2833   C85        C
    2           3        1      3  ...    7.925  None        S
    3           4        1      1  ...     53.1  C123        S
    4           5        0      3  ...     8.05  None        S

    Note
    ------
    Alias the table by `df.table_alias("tabel_name")` before using SQL queries
    """
    return spark.sql(sql_query)


def train_test_split(df, test_size, random_state):
    """The sample set is divided into training set and Validation set

    Parameters
    ----------
    df : spark DataFrame
        Sample set to be splited.

    test_size : float
        The value should be between 0.0 and 1.0 and represent the proportion
        of the dataset to include in the test split.

    random_state : int
        Controls the shuffling applied to the data before applying the split.


    Returns
    --------
    train_df : DataFrame
        Training sample set

    test_df : DataFrame
        Testing sample set


    Examples
    ----------
    >>> df.mega.head(5)
      PassengerId Survived Pclass  ...     Fare Cabin Embarked
    0           1        0      3  ...     7.25  None        S
    1           2        1      1  ...  71.2833   C85        C
    2           3        1      3  ...    7.925  None        S
    3           4        1      1  ...     53.1  C123        S
    4           5        0      3  ...     8.05  None        S
    >>> trainDF, testDF = tm.train_test_split(df, test_size=0.3, random_state=99)
    >>> trainDF.mega.head(5)
          PassengerId Survived Pclass  ...     Fare Cabin Embarked
    0           1        0      3  ...     7.25  None        S
    1          10        1      2  ...  30.0708  None        C
    2         100        0      2  ...       26  None        S
    3         103        0      1  ...  77.2875   D26        S
    4         104        0      3  ...   8.6542  None        S
    """
    train_size = 1 - test_size
    train_df, test_df = df.randomSplit(
        [train_size, test_size], seed=random_state)
    return train_df, test_df


@property
def ext_magellan(self):
    """Converts Spark DataFrame into MegaFrame
    """
    return MegaFrame(self)


DataFrame.mega = ext_magellan
