#!/usr/bin/env python
from setuptools import setup, find_packages
from pipetree import __version__ as pipetree_version
with open('README.rst') as readme_file:
    README = readme_file.read()

    
install_requires = [
    'click==6.6',
]

setup(
    name='pipetree',
    version=pipetree_version,
    description='A minimalist data pipeline library and CLI built on top of Spark.',
    author='Morgan McDermott & John Carlyle',
    long_desciption=README,
    url='https://github.com/mmcdermo/pipetree',
    license='MIT',
    zip_safe=False,
    keywords='pipetree',
    packages=find_packages(),
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'pipetree = pipetree.cli:main',
        ]
    },
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ]
)
