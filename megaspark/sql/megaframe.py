from pyspark.sql.types import ByteType, ShortType, \
    IntegerType, LongType, FloatType, DoubleType, BooleanType, StringType


class MegaFrame(object):

    def __init__(self, df):
        self._df = df
        self.feat_info = [(field.name, field.dataType)
                          for field in self._df.schema.fields]

    def __getitem__(self, *args):
        """Read the DataFrame according to the column names.

        Examples
        ----------
        >>> data_df.mega[["PassengerId", "Survived"]].mega.head(3)
          PassengerId Survived
        0           1        0
        1           2        1
        2           3        1
        """
        col_names = args[0]
        return self._df.select(col_names)

    def head(self, n: int = 5):

        """This function returns the first `n` rows for
        the DataFrame. For negative values of `n`,
        this function returns all rows except
        the last `n` rows, equivalent to ``df[:-n]``.

        Parameters
        ----------
        n : int, default=5
            Number of rows to select.

        Returns
        ---------
        data_df : pandas DataFrame
            The first `n` rows of the caller object.

        Examples
        ----------
        >>> data_df.mega.head(5)
              PassengerId Survived Pclass  ...     Fare Cabin Embarked
        0           1        0      3  ...     7.25  None        S
        1           2        1      1  ...  71.2833   C85        C
        2           3        1      3  ...    7.925  None        S
        3           4        1      1  ...     53.1  C123        S
        4           5        0      3  ...     8.05  None        S
        :param n:
        """
        return self._df.toPandas().head(n)

    def sort_values(self, *col_names, ascending=True):
        """Sort by one or more column names of the data frame

        Parameters
        ----------
        col_names: tuple
            tuple of column names to sort by.

        ascending: bool, default=True
            If True, sort values in ascending order, otherwise descending.

        Returns
        ---------
        data_df: spark DataFrame
            Sorted DataFrame

        Examples
        ----------
        >>> data_df.mega.sort_values("PassengerId", "Survived",
        ... ascending=False).mega.head(3)
          PassengerId Survived Pclass  ...     Fare    Cabin Embarked
        0          99        1      2  ...       23     None        S
        1          98        1      1  ...  63.3583  D10 D12        C
        2          97        0      1  ...  34.6542       A5        C
        """
        return self._df.orderBy(*col_names, ascending=ascending)

    def fillna(self, cols_dict={}):
        """Fill in missing values with fill_value

        Parameters
        ------------
        cols_dict: dict, default={}
            key is colname, value is the missing value to fill in

        Returns
        ------------
        data_df: spark DataFrame
            DataFrame with no missing value

        Examples
        ----------
        >>> data_df.mega.head(5)
          PassengerId Survived Pclass  ...     Fare Cabin Embarked
        0           1        0      3  ...     7.25  None        S
        1           2        1      1  ...  71.2833   C85        C
        2           3        1      3  ...    7.925  None        S
        3           4        1      1  ...     53.1  C123        S
        4           5        0      3  ...     8.05  None        S
        >>> df = data_df.mega.fillna({"Survived": 0, "Cabin": "unknown"})
        >>> df.mega.head(5)
          PassengerId Survived Pclass  ...     Fare    Cabin Embarked
        0           1        0      3  ...     7.25  unknown        S
        1           2        1      1  ...  71.2833      C85        C
        2           3        1      3  ...    7.925  unknown        S
        3           4        1      1  ...     53.1     C123        S
        4           5        0      3  ...     8.05  unknown        S
        """

        if len(cols_dict) == 0:
            for name, dataType in self.feat_info:
                if isinstance(dataType, (
                        ByteType, ShortType, IntegerType, LongType)):
                    cols_dict[name] = 0
                elif isinstance(dataType, (FloatType, DoubleType)):
                    cols_dict[name] = 0.0
                elif isinstance(dataType, (StringType, BooleanType)):
                    cols_dict[name] = "0"
                else:
                    raise Exception("The missing value "
                                    "of the current data "
                                    "type cannot be handled")
        return self._df.na.fill(cols_dict)

    def table_alias(self, name):
        """Creates or replaces a local temporary view with DataFrame.
        The lifetime of this temporary table is tied to `SparkSession`

        Examples
        ----------
        >>> df.table_alias("people")
        """
        self._df.createOrReplaceTempView(name)
