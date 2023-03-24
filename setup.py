# -*- coding:utf-8 -*-

# Name: setup
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/6/9 17:35

from setuptools import setup, find_packages


# python3 setup.py bdist_wheel
setup(
    name='jydata-common',
    version='1.0.0.dev',
    description='Those common packages used in jiayuan data python projects',
    author='zhangjiawen',
    author_email='zhangjiawen@jiayuan.com',
    packages=find_packages(),
    #install_requires=['cx-Oracle>=5.2.1', 'numpy', 'PyMySql', 'requests', 'SQLAlchemy', 'urllib3'],
    include_package_data=True
)
