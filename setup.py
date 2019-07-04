from setuptools import setup, find_packages

setup(
    name = "odkxpy",
    version = "0.1",
    packages = find_packages(),
    install_requires=[
        'pandas', 'suds-jurko', 'requests', 'sqlalchemy'
    ],
)
