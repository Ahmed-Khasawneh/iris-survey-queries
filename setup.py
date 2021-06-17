from setuptools import setup

setup(
    name="iris_report_queries",
    version="0.0.1",
    packages=['common', 'queries'],
    install_requires=['launchdarkly-server-sdk==6.13.2']
)
