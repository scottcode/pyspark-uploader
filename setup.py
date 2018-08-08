#! /usr/bin/env python

from setuptools import setup, find_packages


DISTNAME = 'pyspark_uploader'
MAINTAINER = 'Scott Hajek'
MAINTAINER_EMAIL = 'scott.hajek@alumni.unc.edu'
VERSION = '0.1.0'


if __name__ == '__main__':

    setup(
        name=DISTNAME,
        author=MAINTAINER,
        author_email=MAINTAINER_EMAIL,
        maintainer=MAINTAINER,
        maintainer_email=MAINTAINER_EMAIL,
        version=VERSION,
        packages=find_packages(),
    )
