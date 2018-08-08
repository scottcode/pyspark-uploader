import unittest
import sys
import os
import re

import pandas as pd
import numpy as np

from py4j.protocol import Py4JError, Py4JJavaError

import findspark

findspark.init()

import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as spark_types

# add directory in repo containing package to PYTHONPATH
sys.path.append(os.path.pardir)

from pyspark_uploader.udf import udf_from_module

# add dummy test packages/modules to PYTHONPATH
sys.path.append('resources')

import dummy_package.math
import dummy_module


def main_square(x):
    return x**2


def generate_pandas_df():
    return pd.DataFrame(np.random.rand(20, 5), columns=list('abcde'))


def assertRaisesRegex(testcase, exception_class, regex, callable, *args, **kwds):
    try:
        callable(*args, **kwds)
    except exception_class as e:
        ex = e
    else:
        testcase.fail('Did not raise {}'.format(exception_class.__name__))

    if not re.search(regex, str(ex)):
        testcase.fail(
            'Raised expected exception class but did not contain regex "{}" in message {}'.format(regex, str(ex))
        )


class TestWithoutFails(unittest.TestCase):
    """Test that without the special upload logic the udf doesn't work"""
    def setUp(self):
        self.session = pyspark.sql.SparkSession.builder.getOrCreate()
        self.spark_df = self.session.createDataFrame(generate_pandas_df())

    def tearDown(self):
        self.session.stop()

    def test_package_fails(self):
        square_udf = F.udf(dummy_package.math.square, spark_types.DoubleType())
        spark_df2 = self.spark_df.withColumn('a_sq', square_udf(F.col('a')))
        assertRaisesRegex(self, Py4JJavaError, '(ModuleNotFoundError|ImportError)', spark_df2.toPandas)

    def test_module_fails(self):
        mult_udf = F.udf(dummy_module.mult, spark_types.DoubleType())
        spark_df2 = self.spark_df.withColumn('ab', mult_udf(F.col('a'), F.col('b')))
        assertRaisesRegex(self, Py4JJavaError, '(ModuleNotFoundError|ImportError)', spark_df2.toPandas)

    def test_main_works_without_upload(self):
        main_square_udf = F.udf(main_square, spark_types.DoubleType())
        spark_df2 = self.spark_df.withColumn('ab', main_square_udf(F.col('a')))
        result = spark_df2.toPandas()



class TestWithDoesNotFail(unittest.TestCase):
    """Test that using the udf_from_module works (whereas without it in the previous test case it
    did not work)"""
    def setUp(self):
        self.session = pyspark.sql.SparkSession.builder.getOrCreate()
        self.spark_df = self.session.createDataFrame(generate_pandas_df())

    def tearDown(self):
        self.session.stop()

    def test_module_does_not_fail(self):
        mult_udf = udf_from_module(dummy_module.mult, spark_types.DoubleType(), self.session)
        spark_df2 = self.spark_df.withColumn('ab', mult_udf(F.col('a'), F.col('b')))
        result = spark_df2.toPandas()

    def test_package_does_not_fail(self):
        square_udf = udf_from_module(dummy_package.math.square, spark_types.DoubleType(), self.session)
        spark_df2 = self.spark_df.withColumn('a_sq', square_udf(F.col('a')))
        result = spark_df2.toPandas()

    def test_main_works_with_upload(self):
        main_square_udf = udf_from_module(main_square, spark_types.DoubleType(), self.session)
        spark_df2 = self.spark_df.withColumn('ab', main_square_udf(F.col('a')))
        result = spark_df2.toPandas()


if __name__ == '__main__':
    unittest.main()
