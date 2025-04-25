from setuptools import setup, find_packages

setup(
    name="snow-forts",
    version="0.1.0",
    description="Snowflake infrastructure management toolkit",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    install_requires=[
        "snowflake-connector-python",
        "snowflake-snowpark-python",
        "cryptography",
        "boto3",
        "moto",
        "fakesnow",
        "pytest",
        "pytest-cov"
    ],
    python_requires=">=3.8",
)
