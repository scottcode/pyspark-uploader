#! /usr/bin/env python

from setuptools import setup, find_packages


DESCRIPTION = (
    "Enables rapid development of packages to be used via PySpark on a Spark "
    "cluster by uploading a local Python package to the cluster."
)
DISTNAME = 'pyspark_uploader'
MAINTAINER = 'Scott Hajek'
MAINTAINER_EMAIL = 'scott.hajek@alumni.unc.edu'
VERSION = '0.1.2'


if __name__ == '__main__':

    setup(
        name=DISTNAME,
        description=DESCRIPTION,
        author=MAINTAINER,
        author_email=MAINTAINER_EMAIL,
        maintainer=MAINTAINER,
        maintainer_email=MAINTAINER_EMAIL,
        version=VERSION,
        packages=find_packages(),
    )
