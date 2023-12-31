#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Note: To use the 'upload' functionality of this file, you must:
#   $ pip install twine

import os
import sys
from shutil import rmtree

from setuptools import find_packages, setup, Command

# Package meta-data.
NAME = 'influxdb2-dbapi'
FOLDER = NAME.replace("-","_")
DESCRIPTION = 'Python DB-API and SQLAlchemy interface for influxdb2.'
URL = 'https://github.com/mfyuce/influxdb2-dbapi'
EMAIL = 'fatih.yuce@ulakhaberlesme.com.tr'
AUTHOR = 'Beto Dealmeida + Mehmet F. YUCE'

# What packages are required for this module to be executed?
REQUIRED = [
    'Pygments',
    'requests',
    'six',
    'tabulate',
    'prompt_toolkit',
    'SQLAlchemy',
    'sqlalchemy',
    'influxdb-client',
    'pandas',
    'requests-ntlm',
    'sqlparse',
    'zeep'
]
if sys.version_info < (3, 4):
    REQUIRED.append('enum')

sqlalchemy_extras = [
    'sqlalchemy',
]

cli_extras = [
    'pygments',
    'prompt_toolkit',
    'tabulate',
]

development_extras = [
    'nose',
    'pipreqs',
    'twine',
]

# The rest you shouldn't have to touch too much :)
# ------------------------------------------------
# Except, perhaps the License and Trove Classifiers!
# If you do change the License, remember to change the Trove Classifier for that!

here = os.path.abspath(os.path.dirname(__file__))

long_description = ''

# Load the package's __version__.py module as a dictionary.
about = {}
with open(os.path.join(here, FOLDER, '__version__.py')) as f:
    exec(f.read(), about)


class UploadCommand(Command):
    """Support setup.py upload."""

    description = 'Build and publish the package.'
    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold."""
        print('\033[1m{0}\033[0m'.format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            self.status('Removing previous builds…')
            rmtree(os.path.join(here, 'dist'))
        except OSError:
            pass

        self.status('Building Source and Wheel (universal) distribution…')
        os.system(
            '{0} setup.py sdist bdist_wheel --universal'.format(sys.executable))

        self.status('Uploading the package to PyPi via Twine…')
        os.system('twine upload dist/*')

        sys.exit()


# Where the magic happens:
setup(
    name=NAME,
    version=about['__version__'],
    description=DESCRIPTION,
    long_description=long_description,
    author=AUTHOR,
    author_email=EMAIL,
    url=URL,
    packages=find_packages(exclude=('tests',)),
    # If your package is a single module, use this instead of 'packages':
    # py_modules=['mypackage'],

    entry_points={
        'console_scripts': [
            'influxdb2_db = influxdb2.console:main',
        ],
        'sqlalchemy.dialects': [
            'influxdb2 = influxdb2_dbapi.influxdb2_sqlalchemy:Influxdb2HTTPDialect',
            'influxdb2.http = influxdb2_dbapi.influxdb2_sqlalchemy:Influxdb2HTTPDialect',
            'influxdb2.https = influxdb2_dbapi.influxdb2_sqlalchemy:Influxdb2HTTPSDialect',
        ],
    },
    install_requires=REQUIRED,
    extras_require={
        'dev': development_extras,
        'sqlalchemy': sqlalchemy_extras,
        'cli': cli_extras,
    },
    include_package_data=True,
    license='MIT',
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy'
    ],
    # $ setup.py publish support.
    cmdclass={
        'upload': UploadCommand,
    },
)
