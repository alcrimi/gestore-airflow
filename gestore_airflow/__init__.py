"""Gestore Airflow - DAG management on Google Cloud Composer via the Airflow REST API."""

from .client import AirflowManager

__all__ = ["AirflowManager"]
