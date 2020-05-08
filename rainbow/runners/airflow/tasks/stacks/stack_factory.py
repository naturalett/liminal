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

from rainbow.core.util import class_util
from rainbow.runners.airflow.tasks.stacks.stack import StackTask

__STACK_PACKAGE = 'rainbow/runners/airflow/tasks/stacks'
__USER_STACK_PACKAGE = 'TODO: user_stacks_package'
__RESOURCES_CONF_LIST_KEY = 'resources'
__RESOURCE_CONF_KEY = 'resource'
__RESOURCES_IDS_LIST_KEY = 'resources_ids'
__RESOURCE_ID_KEY = 'resource_id'
__STACK_NAME_KEY = 'stack'

"""
Stack tasks factory
"""


def create_stacks(dag, pipeline, parent):
    """ Generate all required tasks to create stacks"""
    return __handle_stacks_by_method(dag, pipeline, parent, method_type='create')


def delete_stacks(dag, pipeline, parent):
    """ Generate all required tasks to delete stacks"""
    return __handle_stacks_by_method(dag, pipeline, parent, method_type='delete')


def __handle_stacks_by_method(dag, pipeline, parent, method_type):
    stacks = pipeline.get('stacks', [])
    if not stacks:
        print(f'stacks list is empty. skipping {method_type} stacks tasks')
        return None

    start_handle_stacks_task = DummyOperator(dag=dag,
                                             task_id=f'start_{method_type}_all_stacks',
                                             trigger_rule='all_success')
    if parent:
        parent.set_downstream(start_handle_stacks_task)

    parent = start_handle_stacks_task
    end_handle_stacks_task = DummyOperator(dag=dag,
                                           task_id=f'end_{method_type}_all_stacks',
                                           trigger_rule='all_success')

    stack_classes = __find_classes()

    end_handle_stacks_task.set_upstream(
        [__generate_stack_tasks(pipeline=pipeline,
                                stack=stack_task,
                                dag=dag,
                                method_type=method_type,
                                parent=parent,
                                stack_classes=stack_classes) for stack_task in stacks])

    return end_handle_stacks_task


def __generate_stack_tasks(pipeline, stack, dag, method_type, parent, stack_classes):
    if not stack[__RESOURCES_IDS_LIST_KEY]:
        return parent

    stack_name = stack[__STACK_NAME_KEY]

    all_resources_stack = []

    start_handle_stack_tasks = DummyOperator(dag=dag,
                                             task_id=f'start_{method_type}_{stack_name}_stack',
                                             trigger_rule='all_success')
    if parent:
        parent.set_downstream(start_handle_stack_tasks)

    parent = start_handle_stack_tasks

    for resource_id in stack[__RESOURCES_IDS_LIST_KEY]:
        stack[__STACK_NAME_KEY] = f'{resource_id}'
        stack[__RESOURCE_CONF_KEY] = pipeline[__RESOURCES_CONF_LIST_KEY][resource_id]
        stack[__RESOURCE_CONF_KEY][__RESOURCE_ID_KEY] = resource_id

        stack_task_instance = stack_classes[stack['type']](dag, pipeline['pipeline'], parent, stack,
                                                           'all_done', method_type)

        all_resources_stack.append(stack_task_instance.apply_task_to_dag())

    end_handle_stack_tasks = DummyOperator(dag=dag,
                                           task_id=f'end_{method_type}_{stack_name}_stack',
                                           trigger_rule='all_success')

    end_handle_stack_tasks.set_upstream(all_resources_stack)

    return end_handle_stack_tasks


def __find_classes(): return class_util.find_subclasses_in_packages([__STACK_PACKAGE, __USER_STACK_PACKAGE], StackTask)
