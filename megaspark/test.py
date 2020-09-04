# 通过初始化找到本机安装的spark的环境
import findspark
findspark.init()

from pyspark.sql import SparkSession

"""
    Author: huangning.honey@bytedance.com
    Date: 2020/08/21
"""


from copy import deepcopy
from handyspark.ml.base import HandyTransformers
from handyspark.plot import histogram, boxplot, scatterplot, strat_scatterplot, strat_histogram, \
    consolidate_plots, post_boxplot
from handyspark.sql.pandas import HandyPandas
from handyspark.sql.transform import _MAPPING, HandyTransform
from handyspark.util import HandyException, dense_to_array, disassemble, ensure_list, check_columns, \
    none2default
import inspect
from matplotlib.axes import Axes
from collections import OrderedDict
import matplotlib.pyplot as plt
import numpy as np
from operator import itemgetter, add
import pandas as pd
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import Bucketizer
from pyspark.mllib.stat import Statistics
from pyspark.sql import DataFrame, GroupedData, Window, functions as F, Column, Row
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.ml.pipeline import Pipeline
from scipy.stats import chi2
from scipy.linalg import inv


def toHandy(self):
    """Converts Spark DataFrame into HandyFrame.
    """
    return 1


def notHandy(self):
    return self


DataFrame.toHandy = toHandy
DataFrame.notHandy = notHandy

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .master("local") \
        .appName('megaspark') \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    # 读入数据
    spark_path = "../data/train.csv"
    spark_df = spark.read.option("header", "true").csv(spark_path)
    spark_df.show(3)
