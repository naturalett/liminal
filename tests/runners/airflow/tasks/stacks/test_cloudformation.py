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

from rainbow.runners.airflow.operators.cloudformation import CloudFormationCreateStackOperator, \
    CloudFormationCreateStackSensor
from rainbow.runners.airflow.tasks.stacks import cloudformation
from tests.util import dag_test_utils


class TestCloudFormationCreateStackTask(TestCase):

    def test_apply_task_to_dag(self):
        # TODO: elaborate tests
        self.dag = dag_test_utils.create_dag()

        task_id = 'my_task'

        config = self.__create_conf(task_id)

        task0 = cloudformation.CloudFormationStackTask(self.dag, 'my_pipeline', None, config, 'all_done', 'create')
        task0.apply_task_to_dag()

        # Test metadata
        self.assertEqual(task0.stack_name, task_id)
        self.assertEqual(task0.trigger_rule, 'all_done')

        # Test dag tests
        self.assertEqual(len(self.dag.tasks), 4)

        self.__test_is_cloudformation_stack_running(self.dag.tasks[0])

        self.__test_create_cloudformation_stack(self.dag.tasks[1])

        self.__test_cloudformation_watch_cluster_create(self.dag.tasks[2])

        self.__test_stack_creation_end(self.dag.tasks[3])

    def __test_is_cloudformation_stack_running(self, task):
        self.assertIsInstance(task, BranchPythonOperator)
        self.assertEqual(task.task_id, 'is_cloudformation_stack_running')

        downstream_lst = task.downstream_list
        # The order of the downstream tasks here does not matter. sorting just to keep it deterministic for tests
        downstream_lst.sort()
        self.assertEqual(len(downstream_lst), 2)

        self.__test_create_cloudformation_stack(downstream_lst[0])
        self.__test_stack_creation_end(downstream_lst[1])

    def __test_create_cloudformation_stack(self, task):
        self.assertIsInstance(task, CloudFormationCreateStackOperator)
        self.assertEqual(task.task_id, 'create_cloudformation_stack')
        self.assertEqual(task.params['TimeoutInMinutes'], 35)
        self.assertEqual(task.params['TemplateURL'], 'foo bar')
        self.assertEqual(task.params['Parameters'], [{'ParameterKey': 'core_servers', 'ParameterValue': 2},
                                                     {'ParameterKey': 'emr_version', 'ParameterValue': 'emr-5.28.0'}])

    def __test_cloudformation_watch_cluster_create(self, task):
        self.assertIsInstance(task, CloudFormationCreateStackSensor)
        self.assertEqual(task.task_id, 'cloudformation_watch_cluster_create')

    def __test_stack_creation_end(self, task):
        self.assertIsInstance(task, DummyOperator)
        self.assertEqual(task.task_id, 'stack_creation_end')
        self.assertEqual(task.trigger_rule, 'all_done')

    @staticmethod
    def __create_conf(task_id):
        return {
            'name': task_id,
            'template_url': 'foo bar',
            'persistent': False,
            'time_out_in_minutes': 35,
            'parameters': {
                'core_servers': 2,
                'emr_version': 'emr-5.28.0'
            }
        }


if __name__ == '__main__':
    unittest.main()
