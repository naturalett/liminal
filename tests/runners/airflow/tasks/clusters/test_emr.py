from unittest import TestCase

from moto import mock_emr
import unittest
from unittest.mock import MagicMock, patch

from airflow.models.dag import DAG
from airflow.utils import timezone

class TestEMRClusterTask(TestCase):

    @mock_emr
    def test_describe_job_flows(self):
        # TODO: implement tests using moto and boto3
        pass
