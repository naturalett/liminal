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

    def create(self, resource):
        self.__set_global_vars(resource)

        check_cloudformation_stack_exists_task = BranchPythonOperator(
            task_id=f'is_cloudformation_{dynamic_stack_name}_running',
            python_callable=self.__cloudformation_stack_running_branch,
            provide_context=True,
            dag=self.dag
        )

        create_cloudformation_stack_task = CloudFormationCreateStackOperator(
            task_id=create_stack_task_id,
            params={
                **self.__reformatted_params(resource=resource)
            },
            dag=self.dag
        )

        create_stack_sensor_task = CloudFormationCreateStackSensor(
            task_id=f'cloudformation_watch_{dynamic_stack_name}_create',
            stack_name=dynamic_stack_name,
            dag=self.dag
        )

        stack_creation_end_task = DummyOperator(
            task_id=stack_creation_end_task_id,
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

    def delete(self, resource):

        self.__set_global_vars(resource)

        check_dags_queued_task = BranchPythonOperator(
            task_id=f'{dynamic_stack_name}_is_dag_queue_empty',
            python_callable=self.__queued_dag_runs_exists,
            provide_context=True,
            trigger_rule='all_done',
            dag=self.dag
        )

        delete_stack_task = CloudFormationDeleteStackOperator(
            task_id=delete_stack_task_id,
            params={'StackName': dynamic_stack_name},
            dag=self.dag
        )

        delete_stack_sensor = CloudFormationDeleteStackSensor(
            task_id=f'cloudformation_watch_{dynamic_stack_name}_delete',
            stack_name=dynamic_stack_name,
            dag=self.dag
        )

        stack_delete_end_task = DummyOperator(
            task_id=delete_end_task_id,
            dag=self.dag
        )

        if self.parent:
            self.parent.set_downstream(check_dags_queued_task)

        check_dags_queued_task.set_downstream(stack_delete_end_task)
        check_dags_queued_task.set_downstream(delete_stack_task)
        delete_stack_task.set_downstream(delete_stack_sensor)
        delete_stack_sensor.set_downstream(stack_delete_end_task)

        return stack_delete_end_task

    def __reformatted_params(self, resource):
        params_key = 'params'

        self.config[params_key]['StackName'] = dynamic_stack_name

        parameters = self.config[params_key]['Parameters']

        # overwrite or add parameters from resource if exists
        for (p_key, p_value) in FlatDict(resource.get('Parameters', {})):
            parameters[p_key] = p_value

        self.config[params_key]['Parameters'] = [{'ParameterKey': x, 'ParameterValue': y} for (x, y) in
                                                 FlatDict(parameters).items()]
        return self.config[params_key]

    def __cloudformation_stack_running_branch(self, **kwargs):
        cloudformation = CloudFormationHook().get_conn()
        try:
            stack_status = cloudformation.describe_stacks(StackName=dynamic_stack_name)['Stacks'][0]['StackStatus']
            if stack_status in ['CREATE_COMPLETE', 'DELETE_FAILED']:
                print(f'Stack {dynamic_stack_name} is running')
                return stack_creation_end_task_id
            else:
                print(f'Stack {dynamic_stack_name} is not running')
        except Exception as e:
            if 'does not exist' in str(e):
                print(f'Stack {dynamic_stack_name} does not exist')
                return create_stack_task_id
            else:
                raise e

        return create_stack_task_id

    def __queued_dag_runs_exists(self, **kwargs):
        if self.dag.get_num_active_runs() > 1:
            return delete_end_task_id
        else:
            return delete_stack_task_id

    def __set_global_vars(self, resource):
        global dynamic_stack_name, stack_creation_end_task_id, delete_end_task_id, create_stack_task_id, \
            delete_stack_task_id

        resource_id = resource['resource_id']
        # unique key for each resource belongs to this stack
        dynamic_stack_name = f'{self.stack_name}-{resource_id}'

        stack_creation_end_task_id = f'creation_end_{dynamic_stack_name}'
        delete_end_task_id = f'delete_end_{dynamic_stack_name}'
        create_stack_task_id = f'create_cloudformation_{dynamic_stack_name}'
        delete_stack_task_id = f'delete_cloudformation_{dynamic_stack_name}'
