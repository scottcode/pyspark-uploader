# pyspark-uploader
Enables rapid development of packages or modules to be used via PySpark on a 
Spark cluster by uploading a local Python package to the cluster.

Author: Scott Hajek

## Motivation

Functions defined in the `__main__` scope (e.g. in a jupyter notebook or pyspark 
shell) can directly turned into a user-defined function (UDF) and used. However, 
functions imported from modules or packages that do not exist in the cluster 
cannot. Well-established packages that are already in a compress archive 
(tarball) can easily be added to the cluster with `sc.addPyFile()`, but custom 
ones that you are actively developing alongside the pyspark notebook or 
application cannot. You would have to find the path of the package that your 
function came from, zip/targz it, then pass the archive's path to 
`sc.addPyFile`. 

This package facilitates this process by automatically determining the package 
directory where the function was imported from, creating the compressed archive 
in a temp file, running `addPyFile`, and running the UDF definition. 


## Example usage

Assuming you have a development module or package that is in your 
PYTHONPATH, e.g. `dev_pkg`, and a SparkSession instance named 
`spark_session`, then you can do the following:

```
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

from dev_pkg.text import clean_text

from pyspark_uploader.udf import udf_from_module

clean_text_udf = udf_from_module(clean_text, StringType(), spark_session)

df2 = df.withColumn('cleaned', clean_text_udf(F.col('text')))
df2.write.saveAsTable('result_table')
```

Note that without using `udf_from_module` and using `F.udf` instead, the error 
does not occur until an action is called on the resulting dataframe, such as 
`df2.collect()`, `df2.toPandas()`, etc. 
