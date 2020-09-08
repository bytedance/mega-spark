from pyspark.sql import DataFrame
from pyspark.sql.functions import desc
from pyspark.sql import SparkSession

import findspark
findspark.init("/usr/local/Cellar/spark-2.4.6-bin-hadoop2.7")

spark = SparkSession.builder.appName("mega sql").master("local").getOrCreate()

print("welcome Mega Spark 🥳🥳")


def show_func():
    print("+---------------------+")
    print("|ml  method           |")
    print("+---------------------+")
    print("|XGBoostClassifier    |")
    print("+---------------------+")


def read_csv(input_path, header=True):

    # 根据路径创建spark的dataframe
    spark_df = spark.read.option("header", header).csv(input_path)
    return spark_df


def sql(query):
    spark_df = spark.sql(query)
    return spark_df


@property
def ext_magellan(self):
    # 将 Spark DataFrame 转成 MagellanFrame对象
    return MagellanFrame(self)


DataFrame.mega = ext_magellan


class MagellanFrame(object):

    def __init__(self, df, handy=None):
        self._df = df
        self._columns = None

    def __getitem__(self, *args):
        # 取出第一个元素，判断是否是列表
        colnames = args[0]
        res = self._df.select(colnames).toPandas()
        return res

    def table_alias(self, name):
        self._df.createOrReplaceTempView(name)

    def head(self, row_num=10):
        # 取出前几行
        return self._df.toPandas().head()

    def sort_values(self, colname, ascending=True):
        if ascending:
            res = self._df.orderBy("age").toPandas()
            return res
        return self._df.orderBy(desc("age")).toPandas()

    def fillna(self, fill_value):
        spark_df = self._df.na.fill(fill_value)
        return spark_df

    def train_test_split(self, test_size, random_state):
        train_size = 1 - test_size
        train_df, test_df = self._df.randomSplit([train_size, test_size], seed=random_state)
        return train_df, test_df
