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
from flatdict import FlatDict

from rainbow.runners.airflow.operators.cloudformation import CloudFormationCreateStackOperator, \
    CloudFormationCreateStackSensor, CloudFormationHook, CloudFormationDeleteStackSensor, \
    CloudFormationDeleteStackOperator
from rainbow.runners.airflow.tasks.stacks import stack


class CloudFormationStackTask(stack.StackTask):
    """
    Cloudformation stack task
    """

    def __init__(self, dag, pipeline_name, parent, config, trigger_rule, method):
        super().__init__(dag, pipeline_name, parent, config, trigger_rule, method)
        self.tasks_names = self.__set_constant_task_names()

    def create(self):
        check_cloudformation_stack_exists_task = BranchPythonOperator(
            task_id=f'is_cloudformation_{self.stack_name}_running',
            python_callable=self.__cloudformation_stack_running_branch,
            provide_context=True,
            dag=self.dag
        )

        create_cloudformation_stack_task = CloudFormationCreateStackOperator(
            task_id=self.tasks_names.CREATE_STACK_TASK_ID,
            params={
                **self.__reformatted_params()
            },
            dag=self.dag
        )

        create_stack_sensor_task = CloudFormationCreateStackSensor(
            task_id=f'cloudformation_watch_{self.stack_name}_create',
            stack_name=self.stack_name,
            dag=self.dag
        )

        stack_creation_end_task = DummyOperator(
            task_id=self.tasks_names.STACK_CREATION_END_TASK_ID,
            dag=self.dag,
            trigger_rule='all_done'
        )

        if self.parent:
            self.parent.set_downstream(check_cloudformation_stack_exists_task)

        check_cloudformation_stack_exists_task.set_downstream(stack_creation_end_task)
        check_cloudformation_stack_exists_task.set_downstream(create_cloudformation_stack_task)
        create_cloudformation_stack_task.set_downstream(create_stack_sensor_task)

        create_stack_sensor_task.set_downstream(stack_creation_end_task)

        return stack_creation_end_task

    def delete(self):
        check_dags_queued_task = BranchPythonOperator(
            task_id=f'{self.stack_name}_is_dag_queue_empty',
            python_callable=self.__queued_dag_runs_exists,
            provide_context=True,
            trigger_rule='all_done',
            dag=self.dag
        )

        delete_stack_task = CloudFormationDeleteStackOperator(
            task_id=self.tasks_names.DELETE_STACK_TASK_ID,
            params={'StackName': self.stack_name},
            dag=self.dag
        )

        delete_stack_sensor = CloudFormationDeleteStackSensor(
            task_id=f'cloudformation_watch_{self.stack_name}_delete',
            stack_name=self.stack_name,
            dag=self.dag
        )

        stack_delete_end_task = DummyOperator(
            task_id=self.tasks_names.DELETE_END_TASK_ID,
            dag=self.dag
        )

        if self.parent:
            self.parent.set_downstream(check_dags_queued_task)

        check_dags_queued_task.set_downstream(stack_delete_end_task)
        check_dags_queued_task.set_downstream(delete_stack_task)
        delete_stack_task.set_downstream(delete_stack_sensor)
        delete_stack_sensor.set_downstream(stack_delete_end_task)

        return stack_delete_end_task

    def __reformatted_params(self):
        params_key = 'params'
        parameters_key = 'Parameters'

        reformatted_params = self.config[params_key]

        reformatted_params['StackName'] = self.stack_name

        parameters = self.config[params_key][parameters_key]

        # overwrite or add parameters from resource if exists
        for p_key, p_value in FlatDict(self.resource.get(parameters_key, {})).items():
            parameters[p_key] = p_value

        reformatted_params[parameters_key] = [{'ParameterKey': x, 'ParameterValue': y} for (x, y) in
                                              FlatDict(parameters).items()]

        return reformatted_params

    def __cloudformation_stack_running_branch(self, **kwargs):
        cloudformation = CloudFormationHook().get_conn()
        try:
            stack_status = cloudformation.describe_stacks(StackName=self.stack_name)['Stacks'][0]['StackStatus']
            if stack_status in ['CREATE_COMPLETE', 'DELETE_FAILED']:
                print(f'Stack {self.stack_name} is running')
                return self.tasks_names.STACK_CREATION_END_TASK_ID
            else:
                print(f'Stack {self.stack_name} is not running')
        except Exception as e:
            if 'does not exist' in str(e):
                print(f'Stack {self.stack_name} does not exist')
                return self.tasks_names.CREATE_STACK_TASK_ID
            else:
                raise e

        return self.tasks_names.CREATE_STACK_TASK_ID

    def __queued_dag_runs_exists(self, **kwargs):
        if self.dag.get_num_active_runs() > 1:
            return self.tasks_names.DELETE_END_TASK_ID
        else:
            return self.tasks_names.DELETE_STACK_TASK_ID

    def __set_constant_task_names(self):
        class TasksNames(object):
            STACK_CREATION_END_TASK_ID = f'creation_end_{self.stack_name}'
            DELETE_END_TASK_ID = f'delete_end_{self.stack_name}'
            CREATE_STACK_TASK_ID = f'create_cloudformation_{self.stack_name}'
            DELETE_STACK_TASK_ID = f'delete_cloudformation_{self.stack_name}'

        return TasksNames()
