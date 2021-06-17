import setuptools
setuptools.setup(
    name="iris_report_queries",
    version="0.0.1",
    author="",
    author_email="",
    maintainer="",
    maintainer_email="",
    url="",
    description="",
    long_description="",
    long_description_content_type="text/markdown",
    download_url="",
    platforms="",
    license="",
    package_dir={'': '.'},
    packages=['lib', 'queries'],
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    include_package_data=True,
    zip_safe=False,
    install_requires=['launchdarkly-server-sdk==6.13.2']
)
