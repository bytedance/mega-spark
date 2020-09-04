# import findspark
# findspark.init()

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

@property
def ext_magellan(self):
    # 将 Spark DataFrame 转成 MagellanFrame对象
    return MagellanFrame(self)


# 扩展一个toHandy
DataFrame.magellan = ext_magellan


class MagellanFrame(object):

    def __init__(self, df, handy=None):
        self._df = df
        self._columns = None

    # 直接获取某些列的方法
    def __getitem__(self, *args):
        # 取出第一个元素，判断是否是列表
        colnames = args[0]
        res = self._df.select(colnames).toPandas()
        return res

    # head方法
    def head(self, row_num=10):
        # 取出前几行
        return self._df.toPandas().head()

    # sort
    def sort_values(self, colname, ascending=True):
        if ascending:
            res = self._df.orderBy("age").toPandas()
            return res
        return self._df.orderBy(desc("age")).toPandas()