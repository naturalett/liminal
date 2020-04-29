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
from abc import abstractmethod

from airflow.operators.dummy_operator import DummyOperator

from rainbow.runners.airflow.model import task


class StackTask(task.Task):
    """
    Stack task
    """

    def __init__(self, dag, pipeline_name, parent, config, trigger_rule, method):
        super().__init__(dag, pipeline_name, parent, config, trigger_rule)
        self.method = method
        self.stack_name = self.config['stack']
        self.resources = self.config['resources']

    def apply_task_to_dag(self):
        """
        Create one unique stack for each resource in config[resources_ids] list.
        Note: All stack#resources share the same stack properties

        """
        start_stack_resources_tasks = DummyOperator(dag=self.dag,
                                                    task_id=f'start_{self.stack_name}_{self.method}_tasks',
                                                    trigger_rule='all_success')

        if self.parent:
            self.parent.set_downstream(start_stack_resources_tasks)

        self.parent = start_stack_resources_tasks

        stack_all_resources_tasks = [self.__apply_tasks_to_dag(resource_id) for resource_id in
                                     self.config['resources_ids']]

        end_stack_resources_tasks = DummyOperator(dag=self.dag,
                                                  task_id=f'end_{self.stack_name}_{self.method}_tasks',
                                                  trigger_rule='all_success')

        end_stack_resources_tasks.set_upstream(stack_all_resources_tasks)

        return end_stack_resources_tasks

    def __apply_tasks_to_dag(self, resource_id):
        resource_params = self.resources[resource_id]
        resource_params['resource_id'] = resource_id
        return getattr(self, self.method)(resource=resource_params)

    @abstractmethod
    def create(self, resource):
        raise NotImplementedError()

    @abstractmethod
    def delete(self, resource):
        raise NotImplementedError()
