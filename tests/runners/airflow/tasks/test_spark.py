from unittest import TestCase

from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

from rainbow.runners.airflow.tasks import spark
from tests.util import dag_test_utils


class TestSparkTask(TestCase):
    def test_apply_task_to_dag(self):

        dag = dag_test_utils.create_dag()

        task_id = 'my_spark_task'

        config = self.__create_conf(task_id)

        task0 = spark.SparkTask(dag, 'my_pipeline', None, config, 'all_success')

        task0.apply_task_to_dag()

        self.assertEqual(len(dag.tasks), 2)

        dag_task0 = dag.tasks[0]
        self.assertIsInstance(dag_task0, EmrAddStepsOperator)

        self.assertEqual(dag_task0.aws_conn_id, 'aws_conn_id')

        self.assertEqual(dag_task0.cluster_states, ['RUNNING', 'WAITING'])

        self.assertEqual(dag_task0.job_flow_name, 'cluster_name_id')

        dag_task1 = dag.tasks[1]
        self.assertIsInstance(dag_task1, EmrStepSensor)

        expected_spark_submit = ['spark-submit', '--name', 'hello_spark', '--deploy-mode', 'client', '--class',
                                 '.class', '--conf', 'spark.sql.parquet.writeLegacyFormat=true', '--conf',
                                 'spark.driver.cores=3', 'source_jar', 'application-id', 'my_spark_test_id', '--env',
                                 'env', '--cloudwatch-reporting-enabled', '', '--audit-reporting-enabled', '']

        expected_emr_steps = [
            {
                'Name': task0.task_name,
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': expected_spark_submit
                }
            }
        ]

        self.assertEqual(dag_task0.steps, expected_emr_steps)

        self.assertEqual(task0.spark_submit, expected_spark_submit)

    @staticmethod
    def __create_conf(task_id):
        return {
            'task': task_id,
            'source_path': 'source_jar',
            'spark_arguments': {
                'name': 'hello_spark',
                'deploy-mode': 'client',
                'class': '.class',
                'conf': {
                    'spark.sql.parquet.writeLegacyFormat': 'true',
                    'spark.driver.cores': 3
                }
            },
            'application_arguments': {
                'application-id': 'my_spark_test_id',
                '--env': 'env',
                '--cloudwatch-reporting-enabled': '',
                '--audit-reporting-enabled': '',
            },
            'stack_id': 'cloudformation_emr_id',

            'resources': {
                'cloudformation_emr_id': {
                    'cluster': {
                        'type': 'emr',
                        'parameters': {
                            'cluster_name': 'cluster_name_id',
                            'aws_conn_id': 'aws_conn_id',
                            'cluster_states': ['RUNNING', 'WAITING']
                        }
                    }
                }
            }
        }
