import tarfile
import tempfile
import os
import pydoc

import pyspark
import pyspark.sql.functions as F


def get_module_type(func):
    if func.__module__ == '__main__':
        return 'main'
    module_breadcrumbs = func.__module__.split('.')
    if len(module_breadcrumbs) == 1:
        return 'module'
    location = pydoc.locate(module_breadcrumbs[0]).__file__
    if os.path.basename(location) == '__init__.py':
        return 'package'
    else:
        raise ValueError('did not recognize module type')


def get_pkg_name(func):
    return func.__module__.split('.')[0]


def get_path_to_pkg(func):
    # pydoc.locate trick adapted from https://stackoverflow.com/a/24815361/1789708
    path = pydoc.locate(get_pkg_name(func)).__file__
    if os.path.basename(path) == '__init__.py':
        path = os.path.dirname(path)
    return path


def udf_from_module(func, return_type, spark_sess_or_context, path_to_package=None):
    if path_to_package is None:
        path_to_package = get_path_to_pkg(func)

    module_type = get_module_type(func)
    if module_type == 'package':
        package_name = get_pkg_name(func)
    elif module_type == 'module':
        package_name = os.path.basename(path_to_package)
    elif module_type == 'main':
        # modules defined in the __main__ module don't need to be uploaded
        return F.udf(func, return_type)
    else:
        raise NotImplementedError('module_type not recognized: {}'.format(module_type))

    with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as tf_:
        with tarfile.open(tf_.name, mode='w:gz') as arch_:
            arch_.add(path_to_package, arcname=package_name)
    if isinstance(spark_sess_or_context, pyspark.sql.SparkSession):
        spark_sess_or_context.sparkContext.addPyFile(tf_.name)
    elif isinstance(spark_sess_or_context, pyspark.SparkContext):
        spark_sess_or_context.addPyFile(tf_.name)
    else:
        raise TypeError(
            "spark_sess_or_context must be SparkSession or SparkContext, but instead got {}".format(
                type(spark_sess_or_context)
            )
        )
    return F.udf(func, return_type)
