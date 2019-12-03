import os
from setuptools import setup, find_packages

BASEDIR = os.path.dirname(os.path.abspath(__file__))
VERSION = open(os.path.join(BASEDIR, 'VERSION')).read().strip()

BASE_DEPENDENCIES = [
    'wf-database-connection>=0.1.1',
    'wf-minimal-honeycomb-python>=0.3.0',
    'python-dateutil>=2.8.0'
]

# allow setup.py to be run from any path
os.chdir(os.path.normpath(BASEDIR))

setup(
    name='wf-database-connection-honeycomb',
    packages=find_packages(),
    version=VERSION,
    include_package_data=True,
    description='An implementation of the database_connection API using Wildflower\'s Honeycomb database',
    long_description=open('README.md').read(),
    url='https://github.com/WildflowerSchools/database_connection_honeycomb',
    author='Theodore Quinn',
    author_email='ted.quinn@wildflowerschools.org',
    install_requires=BASE_DEPENDENCIES,
    keywords=['database'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ]
)
