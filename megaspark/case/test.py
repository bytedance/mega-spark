import megaspark.tomega as tm

if __name__ == "__main__":
    input_path = "/Users/bytedance/ByteCode/magellan_megaspark/data/train.csv"
    data_df = tm.read_csv(input_path, header=True)

    data_df.mega.table_alias("student")
    sample_df = tm.sql("select * from student")
    df = sample_df.mega.fillna({"Survived": 0, "Cabin": "unknown"})
    print(df.mega.head(5))
