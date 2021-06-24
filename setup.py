from setuptools import setup, find_packages

install_requirements = [
    "apache-airflow",
    "prometheus_client>=0.4.2",
]

setup(
    description="Prometheus metrics for Data-Flow",
    install_requires=install_requirements,
    keywords="data_flow_metrics",
    name="data_flow_metrics",
    packages=find_packages(include=["data_flow_metrics"]),
    include_package_data=True,
    version="0.0.1",
    entry_points={
        "airflow.plugins": [
            "DataFlowPrometheus = data_flow_metrics.data_flow_metrics:DataFlowPlugin"
        ]
    },
)
