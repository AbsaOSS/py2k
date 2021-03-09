#!/usr/bin/env python

# Copyright 2021 ABSA Group Limited

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('HISTORY.md') as history_file:
    history = history_file.read()

requirements = [
    'confluent_kafka[avro] >= 1.5.0',
    'pandas >= 1.1.0',
    'pydantic >= 1.5.1',
    'tqdm >= 4.48.2'
]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest>=3', ]

setup(
    author="Daniel Wertheimer",
    author_email='daniel.wertheimer@absa.africa',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="High level Python API for writing to Kafka",
    install_requires=requirements,
    license="Apache Software license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='py2k',
    name='py2k',
    packages=find_packages(
        include=['py2k', 'py2k.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/AbsaOSS/py2k.git',
    version='1.6.0',
    zip_safe=False,
)
