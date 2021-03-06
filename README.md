# Mega Spark说明
该项目旨在通过spark进行一站式数据分析与模型训练，保证最终落地的只有分析报告，可视化，以及模型训练评估结果，其次
该项目将pysaprk封装成mega对象来延续pandas的使用方法，进而实现在大数据场景下的使用pandas方法进行
数据分析和模型训练，消除相关同学在spark上投入的学习成本

目前提供以下模块：
* `ml`
    * `mega_xgboost`
* `sql`
    * `megaframe`
* `tomega` 
  
  
# 本地安装

如果要给该项目贡献代码，在本地调试好后测试，本地安装方法

```
$ git clone .
$ cd megaspark
$ python install .
```

# 在线安装
建议使用官方镜像，安装最新版本。

```
$ pip install --index-url https://pypi.org/simple/ mega-spark
```

# 使用教程
以`tomega`模块为例，安装完成之后，可以使用以下方法导入使用

```
import megaspark.tomega as mg

data_df = mg.read_csv("path/to/file.csv")
data_df.mega.head(5)
```

# 打包发布

```
$ cd /path/to/megaspark
$ python3 setup.py sdist bdist_wheel
$ pip3 install twine
$ python3 -m twine upload dist/*
```

# 注意事项

如果使用`ml`模块中的`xgboost`，需要在`SPARK_HOME/jar`中添加`xgboost4j-0.72.jar`以及`xgboost4j-spark-0.72.jar`

