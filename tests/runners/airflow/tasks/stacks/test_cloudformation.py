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

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils import timezone
from jsonpickle import json
from mock import MagicMock

from rainbow.runners.airflow.operators.cloudformation import CloudFormationCreateStackOperator, \
    CloudFormationCreateStackSensor
from rainbow.runners.airflow.tasks.stacks import cloudformation_stack
from tests.util import dag_test_utils

DEFAULT_DATE = timezone.datetime(2019, 1, 1)


class TestCloudFormationStackTask(TestCase):
    """
    Test create/delete of cloudformation_stack
    """
    # TODO: Write tests
    # def setUp(self):
    #     pass
    #
    # def test_apply_task_to_dag(self):
    #
    #     global stack_idx, stack_name, num_stack_resources_tasks
    #     num_stack_resources_tasks = 4
    #     stack_idx = 1
    #
    #     self.task_id = 'my-task'
    #     config = self.__create_conf(self.task_id)
    #     for stack_task in config['stacks']:
    #         stack_task['resources'] = config['resources']
    #         stack_name = f'{self.task_id}-{stack_idx}'
    #         self.__test_create_stack(stack_task)
    #         self.__test_delete_stack(stack_task)
    #         stack_idx += 1
    #
    # def __test_create_stack(self, stack_task):
    #     self.__init_dag()
    #     # Mock out the cloudformation_client (moto fails with an exception).
    #     self.cloudformation_client_mock = MagicMock()
    #
    #     self.mock_context = MagicMock()
    #
    #     task = cloudformation_stack.CloudFormationStackTask(self.dag, 'my_pipeline', None, stack_task, 'all_done',
    #                                                         'create')
    #
    #     self.assertEqual(task.stack_name, stack_name)
    #     self.assertEqual(task.trigger_rule, 'all_done')
    #     self.assertIsNotNone(task.config['resources_ids'])
    #
    #     task.apply_task_to_dag()
    #
    #     # 4 tasks for each resource and another 2 shared tasks
    #     self.assertEqual(len(self.dag.tasks), num_stack_resources_tasks * stack_idx + 2)
    #
    #     self.__test_shared_stack_tasks('create')
    #
    #     self.__test_create_stack_tasks(task)
    #
    # def __test_delete_stack(self, stack_task):
    #
    #     self.__init_dag()
    #
    #     task = cloudformation_stack.CloudFormationStackTask(self.dag, 'my_pipeline', None, stack_task, 'all_done',
    #                                                         'delete')
    #     self.assertEqual(task.stack_name, stack_name)
    #     self.assertEqual(task.trigger_rule, 'all_done')
    #     self.assertIsNotNone(task.config['resources_ids'])
    #
    #     task.apply_task_to_dag()
    #
    #     # 4 tasks for each resource and another 2 shared tasks
    #     self.assertEqual(len(self.dag.tasks), num_stack_resources_tasks * stack_idx + 2)
    #
    #     self.__test_shared_stack_tasks('delete')
    #
    # def __test_create_stack_tasks(self, task):
    #     i = 1
    #     for resource_id in task.config['resources_ids']:
    #         global dynamic_stack_name
    #         dynamic_stack_name = f'{stack_name}-{resource_id}'
    #         self.__test_is_cloudformation_stack_running(self.dag.tasks[i])
    #         i = i + num_stack_resources_tasks
    #
    # def __test_is_cloudformation_stack_running(self, task):
    #
    #     self.assertIsInstance(task, BranchPythonOperator)
    #
    #     self.assertEqual(task.task_id, f'is_cloudformation_{dynamic_stack_name}_running')
    #
    #     downstream_lst = task.downstream_list
    #     # The order of the downstream tasks here does not matter. sorting just to keep it deterministic for tests
    #     downstream_lst.sort()
    #     self.assertEqual(len(downstream_lst), 2)
    #
    #     self.__test_create_cloudformation_stack(downstream_lst[0])
    #     self.__test_stack_creation_end(downstream_lst[1])
    #
    # def __test_create_cloudformation_stack(self, task):
    #
    #     self.assertIsInstance(task, CloudFormationCreateStackOperator)
    #     self.assertEqual(task.task_id, f'create_cloudformation_{dynamic_stack_name}')
    #
    #     task.execute(self.mock_context)
    #
    #     self.cloudformation_client_mock.create_stack.assert_any_call(StackName=dynamic_stack_name,
    #                                                                  TimeoutInMinutes=stack_idx)
    #
    #     self.__test_cloudformation_watch_cluster_create(task.downstream_list[0])
    #
    # def __test_cloudformation_watch_cluster_create(self, task):
    #     self.assertIsInstance(task, CloudFormationCreateStackSensor)
    #     self.assertEqual(task.task_id, f'cloudformation_watch_{dynamic_stack_name}_create')
    #
    # def __test_stack_creation_end(self, task):
    #     self.assertIsInstance(task, DummyOperator)
    #     self.assertEqual(task.task_id, f'creation_end_{dynamic_stack_name}')
    #     self.assertEqual(task.trigger_rule, 'all_done')
    #
    # def __test_shared_stack_tasks(self, method_type):
    #     start_stack_task = self.dag.tasks[0]
    #     self.assertIsInstance(start_stack_task, DummyOperator)
    #     self.assertEqual(start_stack_task.task_id, f'start_{stack_name}_{method_type}_tasks')
    #
    #     end_stack_task = self.dag.tasks[len(self.dag.tasks) - 1]
    #     self.assertIsInstance(end_stack_task, DummyOperator)
    #     self.assertEqual(end_stack_task.task_id, f'end_{stack_name}_{method_type}_tasks')
    #
    # def __init_dag(self):
    #     self.dag = dag_test_utils.create_dag()

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
                    'overwrite': {
                        'parameters': {
                            'myParam1': "2",
                            'myParam3': "3"
                        }
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
    "Description": "Stack 1",
    "Resources": {
        "EC2Instance1": {
            "Type": "AWS::EC2::Instance",
            "Properties": {
                "ImageId": "ami-d3adb33f",
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
