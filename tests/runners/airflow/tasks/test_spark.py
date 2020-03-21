from unittest import TestCase

from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

from rainbow.runners.airflow.tasks import spark
from tests.util import dag_test_utils


class TestSparkTask(TestCase):
    def test_apply_task_to_dag(self):
        # TODO: elaborate tests
        dag = dag_test_utils.create_dag()

        task_id = 'my_spark_task'

        config = self.__create_conf(task_id)

        task0 = spark.SparkTask(dag, 'my_pipeline', None, config, 'all_success')

        task0.apply_task_to_dag()

        self.assertEqual(len(dag.tasks), 2)

        dag_task0 = dag.tasks[0]
        self.assertIsInstance(dag_task0, EmrAddStepsOperator)

        dag_task1 = dag.tasks[1]
        self.assertIsInstance(dag_task1, EmrStepSensor)

        expected_spark_submit = ['spark-submit', '--name', 'hello_spark', '--deploy_mode', 'standalone', '--class',
                                 '.class', '--conf', 'spark.sql.parquet.writeLegacyFormat=true', 'source_jar',
                                 '--ni-main-class', 'main_class', '--ni-application-id', 'application_id', '--env',
                                 'env', '--cloudwatch-reporting-enabled', '', '--audit-reporting-enabled', '']

        self.assertEqual(task0.spark_submit, expected_spark_submit)

    @staticmethod
    def __create_conf(task_id):
        return {
            'task': task_id,
            'name': 'test_spark_task',
            'source_path': 'source_jar',
            'spark_arguments': {
                'name': 'hello_spark',
                'deploy_mode': 'standalone',
                'class': '.class',
                'conf': {
                    'spark.sql.parquet.writeLegacyFormat': 'true'
                }
            },
            'application_arguments': {
                'ni-main-class': 'main_class',
                'ni-application-id': 'application_id',
                'env': 'env',
                'cloudwatch-reporting-enabled': '',
                'audit-reporting-enabled': '',
            },
            'resource': {
                'type': 'emr',
                'parameters': {
                    'aws_conn_id': 'awn-ni',
                    'cluster_states': ['RUNNING', 'WAITING']
                }
            }
        }
