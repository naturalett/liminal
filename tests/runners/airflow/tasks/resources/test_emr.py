from unittest import TestCase

from moto import mock_emr


class TestEmrResourceTask(TestCase):

    @mock_emr
    def test_describe_job_flows(self):
        # TODO: implement tests using moto and boto3
        pass
