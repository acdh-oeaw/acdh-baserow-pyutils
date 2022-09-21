#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()


requirements = [
    'pandas>=1.4.0,<2',
    'requests>=2.20.0,<3'
]

setup_requirements = []

test_requirements = []

setup(
    author="Peter Andorfer",
    author_email='peter.andorfer@oeaw.ac.at',
    python_requires='>=3.8',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
    ],
    description="Utility functions to work with Baserow",
    install_requires=requirements,
    license="MIT license",
    long_description=readme,
    long_description_content_type='text/markdown',
    include_package_data=True,
    package_data={
        '': ['fixtures/*.*'],
        'acdh_collatex_utils': ['xslt/*.xsl']
    },
    name='acdh_baserow_pyutils',
    packages=find_packages(include=['acdh_baserow_pyutils', 'acdh_baserow_pyutils.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/acdh-oeaw/acdh-baserow-pyutils',
    version='0.0.1',
    zip_safe=False,
)
