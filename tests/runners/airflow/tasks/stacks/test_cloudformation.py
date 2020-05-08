#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest
from unittest import TestCase

from airflow.utils import timezone
from jsonpickle import json
from mock import MagicMock, patch

from rainbow.runners.airflow.operators.cloudformation import CloudFormationCreateStackOperator, \
    CloudFormationDeleteStackOperator
from rainbow.runners.airflow.tasks.stacks import stack_factory
from tests.util import dag_test_utils

DEFAULT_DATE = timezone.datetime(2019, 1, 1)


class TestCloudFormationStackTask(TestCase):
    """
    Test create/delete of cloudformation_stack
    """

    def setUp(self):
        self.task_id = 'my-task'
        self.config = self.__create_conf(self.task_id)
        self.dag = dag_test_utils.create_dag()

        # Set up Mock

        # Mock out the cloudformation_client (moto fails with an exception).
        self.cloudformation_client_mock = MagicMock()

        # Mock out the emr_client creator
        cloudformation_session_mock = MagicMock()
        cloudformation_session_mock.client.return_value = self.cloudformation_client_mock
        self.boto3_session_mock = MagicMock(return_value=cloudformation_session_mock)
        self.mock_context = MagicMock()

        self.config['pipeline'] = 'my-pipe'

    def test_apply_task_to_dag_create(self):
        stack_factory.create_stacks(dag=self.dag, pipeline=self.config, parent=None)

        self.__test_create_stack_operator(self.dag.tasks[4])

    def test_apply_task_to_dag_delete(self):
        stack_factory.delete_stacks(dag=self.dag, pipeline=self.config, parent=None)

        self.assertEqual(self.dag.tasks[0].task_id, 'start_delete_all_stacks')
        self.assertEqual(self.dag.tasks[1].task_id, 'end_delete_all_stacks')
        self.assertEqual(self.dag.tasks[2].task_id, 'start_delete_my-task-1_stack')
        self.assertEqual(self.dag.tasks[3].task_id, 'mycluster-20_is_dag_queue_empty')

        self.__test_delete_stack_operator(self.dag.tasks[4])

    def __test_delete_stack_operator(self, task):
        self.assertIsInstance(task, CloudFormationDeleteStackOperator)
        self.assertEqual(task.task_id, 'delete_cloudformation_mycluster-20')

        with patch('boto3.session.Session', self.boto3_session_mock):
            task.execute(self.mock_context)

        self.cloudformation_client_mock.delete_stack.assert_any_call(StackName='mycluster-20')

    def __test_create_stack_operator(self, task):
        self.assertIsInstance(task, CloudFormationCreateStackOperator)
        self.assertEqual(task.task_id, 'create_cloudformation_mycluster-20')
        with patch('boto3.session.Session', self.boto3_session_mock):
            task.execute(self.mock_context)

        self.cloudformation_client_mock.create_stack.assert_any_call(
            TemplateBody='{"AWSTemplateFormatVersion": "2010-09-09", "Description": "TemplateBody",'
                         ' "Parameters":'
                         ' {"myParam1": {"Description": "Test my_param_1", "Type": "String"}, "myParam2":'
                         ' {"Description": "Test my_param_2", "Type": "String"}}, "Resources":'
                         ' {"EC2Instance1": {"Properties": {"KeyName": "dummy"}, "Type": "emr"}}}',
            TimeoutInMinutes=1,
            Parameters=[{'ParameterKey': 'myParam1', 'ParameterValue': '2'},
                        {'ParameterKey': 'myParam2', 'ParameterValue': 'myParam2'},
                        {'ParameterKey': 'myParam3', 'ParameterValue': '3'}],
            StackName='mycluster-20')

    @staticmethod
    def __create_conf(task_id) -> dict:
        return {
            'stacks': [
                {
                    'stack': f'{task_id}-1',
                    'type': 'cloudformation_stack',
                    'params': {
                        'TemplateBody': json.dumps(dummy_template),
                        'TimeoutInMinutes': 1,
                        'Parameters': {
                            'myParam1': "1",
                            'myParam2': "myParam2",
                        },
                    },
                    'resources_ids': [
                        'mycluster-20'
                    ]
                },
            ],

            'resources': {
                'mycluster-20': {
                    'type': 'emr',
                    'aws_conn_id': 'aws_conn_id_1',
                    'Parameters': {
                        'myParam1': "2",
                        'myParam3': "3"
                    }
                },
                'mycluster-21': {
                    'type': 'emr',
                    'aws_conn_id': 'aws_conn_id_2_2'
                },
                'mydatabase': {
                    'type': 'mysql'
                }
            }
        }

    if __name__ == '__main__':
        unittest.main()


dummy_template = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Description": "TemplateBody",
    "Resources": {
        "EC2Instance1": {
            "Type": "emr",
            "Properties": {
                "KeyName": "dummy"
            }
        }
    },
    'Parameters': {
        'myParam1': {
            'Description': 'Test my_param_1',
            'Type': 'String'
        },
        'myParam2': {
            'Description': 'Test my_param_2',
            'Type': 'String'
        }
    }
}
