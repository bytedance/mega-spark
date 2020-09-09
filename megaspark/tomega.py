import findspark
findspark.init("/usr/local/Cellar/spark-2.4.6-bin-hadoop2.7")

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from megaspark.sql.megaframe import MegaFrame

spark = SparkSession.builder.appName("mega sql").master("local").getOrCreate()

print("welcome Mega Spark 🥳🥳")


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


def read_csv(input_path, header=True):
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

    return spark.read.option("header", header).csv(input_path)


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


@property
def ext_magellan(self):
    """Converts Spark DataFrame into MegaFrame
    """
    return MegaFrame(self)


DataFrame.mega = ext_magellan
