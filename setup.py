from setuptools import find_packages, setup

setup(
    name="dagster_quickstart",
    packages=find_packages(include=["dagster_quickstart", "dagster_se_interview", "dagster_quickstart.*", "dagster_se_interview.*"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "scikit-learn"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
