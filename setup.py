from setuptools import setup

setup(
    name='reconcile',
    version='0.1.0',
    py_modules=['reconcile'],
    install_requires=[
        'Click',
    ],
    entry_points={
        'console_scripts': [
            'reconcile = reconcile:run',
        ],
    },
)