import megaspark.tomega as tm
from megaspark.ml.mega_xgboost import XGBoostClassifier

if __name__ == "__main__":
    input_path = "/Users/bytedance/ByteCode/magellan_megaspark/data/train.csv"
    data_df = tm.read_csv(input_path, header=True, feats_info={"PassengerId": "double",
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

    data_df = data_df.mega.fillna({"PassengerId": 0,
                                   "Survived": 0,
                                   "Pclass": 0,
                                   "Name": "0",
                                   "Sex": "0",
                                   "Age": 0,
                                   "SibSp": 0,
                                   "Parch": 0,
                                   "Ticket": "0",
                                   "Fare": 0,
                                   "Cabin": "0",
                                   "Embarked": "0"})

    # data_df = data_df.na.fill(0)
    data_df.mega.table_alias("student")

    # # sql method
    # sample_df = tm.sql("select * from student limit 10")

    # split dataset into trainDF and testDF
    trainDF, testDF = tm.train_test_split(data_df, test_size=0.3, random_state=99)

    # model training
    xgb_clf = XGBoostClassifier("features", "Survived", "prediction")
    xgb_clf.fit(trainDF)
    res = xgb_clf.predict_proba(trainDF)
    print(res.mega.head(5))
