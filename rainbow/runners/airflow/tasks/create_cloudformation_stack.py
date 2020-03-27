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
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

from rainbow.runners.airflow.model import task
from rainbow.runners.airflow.operators.cloudformation import CloudFormationHook, CloudFormationCreateStackOperator, \
    CloudFormationCreateStackSensor

from flatdict import FlatDict


class CreateCloudFormationStackTask(task.Task):
    """
    Creates cloud_formation stack.
    """

    def __init__(self, dag, pipeline_name, parent, config, trigger_rule):
        super().__init__(dag, pipeline_name, parent, config, trigger_rule)
        self.stack_name = config['name']
        self.template_url = config['template_url']
        self.time_out_in_minutes = config.get('time_out_in_minutes', 25)
        self.parameters = self.__get_parameters()

    def apply_task_to_dag(self):

        check_cloudformation_stack_exists_task = BranchPythonOperator(
            task_id='is_cloudformation_stack_running',
            python_callable=self.__cloudformation_stack_running_branch,
            provide_context=True,
            dag=self.dag
        )

        create_cloudformation_stack_task = CloudFormationCreateStackOperator(
            task_id='create_cloudformation_stack',
            params={
                'StackName': self.stack_name,
                'TimeoutInMinutes': self.time_out_in_minutes,
                'Capabilities': ['CAPABILITY_NAMED_IAM'],
                'TemplateURL': self.template_url,
                'Parameters': self.parameters,
                'OnFailure': 'DO_NOTHING'
            },
            dag=self.dag
        )

        create_stack_sensor_task = CloudFormationCreateStackSensor(
            task_id='cloudformation_watch_cluster_create',
            stack_name=self.stack_name,
            dag=self.dag
        )

        stack_creation_end_task = DummyOperator(
            task_id='stack_creation_end',
            dag=self.dag,
            trigger_rule=self.trigger_rule
        )

        if self.parent:
            self.parent.set_downstream(check_cloudformation_stack_exists_task)

        check_cloudformation_stack_exists_task.set_downstream(stack_creation_end_task)
        check_cloudformation_stack_exists_task.set_downstream(create_cloudformation_stack_task)
        create_cloudformation_stack_task.set_downstream(create_stack_sensor_task)

        create_stack_sensor_task.set_downstream(stack_creation_end_task)

    def __cloudformation_stack_running_branch(self, **kwargs):
        cloudformation = CloudFormationHook().get_conn()
        try:
            stack_status = cloudformation.describe_stacks(StackName=self.stack_name)['Stacks'][0]['StackStatus']
            if stack_status in ['CREATE_COMPLETE', 'DELETE_FAILED']:
                print(f'Stack {self.stack_name} is running')
                return 'stack_creation_end'
            else:
                print(f'Stack {self.stack_name} is not running')
        except Exception as e:
            if 'does not exist' in str(e):
                print(f'Stack {self.stack_name} does not exist')
                return 'create_cloudformation_stack'
            else:
                raise e

        return 'create_cloudformation_stack'

    def __get_parameters(self):
        return [{'ParameterKey': x, 'ParameterValue': y} for (x, y) in
                FlatDict(self.config['parameters']).items()]
