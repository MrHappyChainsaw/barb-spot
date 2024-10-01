from setuptools import setup, find_packages

setup(
    name='station_price',
    version='0.1',
    description='A Python package for comparin Station Price Spot Data',
    author='S Tarry',
    author_email='',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'pandas',
        'pyodbc',  # or sqlalchemy, depending on what you're using
        'python-decouple',
    ],
)