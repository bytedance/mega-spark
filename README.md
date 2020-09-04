# MegaSpark说明
这是一个旨在`...`，定期发布到`pipy.org`

目前提供以下模块：
* `ml`
* `sql`
* `entrance` 
  
  
```
# 本地安装
如果要给该项目贡献代码，在本地调试好后测试，本地安装方法

```python
$ git clone ...
$ cd megaspark
$ python install .
```

# 使用教程
以`magllan_ai.ml.mag_util.mag_metrics`模块为例，安装完成之后，可以使用以下方法导入使用

```
from libs.magellanai import *

data_df = read_csv("path/to/file.csv")
data_df.magellan.head(5)
```

# 打包发布

```
$ cd /path/to/megaspark
$ python setup.py sdist bdist_wheel
$ pip install twine
$ twine upload dist/*
```