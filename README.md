# pyspark-uploader
Enables rapid development of packages to be used via PySpark on a Spark cluster by uploading a local Python package to the cluster.

Author: Scott Hajek


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

