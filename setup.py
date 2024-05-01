from setuptools import setup

setup(
    name='reconciii',
    version='0.1.0',
    py_modules=['reconcile'],
    author='Ibrahim hamzat',
    author_email='hamat.ibrahim3@gmail.com',
    description='A csv reconciliation tool and report generator',
    install_requires=[
        'Click',
    ],
    entry_points={
        'console_scripts': [
            'reconcile = reconcile:run',
        ],
    },
)