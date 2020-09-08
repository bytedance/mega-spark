import megaspark.sql.entrymega as mg

if __name__ == "__main__":

    input_path = "/Users/bytedance/ByteCode/magellan_megaspark/data/train.csv"
    data_df = mg.read_csv(input_path)

    # 给hive表的数据框起一个别名
    data_df.mega.table_alias("student")

    sample_df = mg.sql("select * from student")

    sample_df.show(3)
