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

from datetime import datetime, timedelta

import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from rainbow.core.util import class_util
from rainbow.core.util import files_util
from rainbow.runners.airflow.model.task import Task
from rainbow.runners.airflow.tasks.defaults.job_end import JobEndTask
from rainbow.runners.airflow.tasks.defaults.job_start import JobStartTask
from rainbow.runners.airflow.tasks.stacks.stack import StackTask

__DEPENDS_ON_PAST = 'depends_on_past'


def register_dags(configs_path):
    """
    Registers pipelines in rainbow yml files found in given path (recursively) as airflow DAGs.
    """

    config_files = files_util.find_config_files(configs_path)

    dags = []

    for config_file in config_files:
        print(f'Registering DAG for file: {config_file}')

        with open(config_file) as stream:
            config = yaml.safe_load(stream)

            for pipeline in config['pipelines']:
                pipeline_name = pipeline['pipeline']

                default_args = {k: v for k, v in pipeline.items()}

                override_args = {
                    'start_date': datetime.combine(pipeline['start_date'], datetime.min.time()),
                    __DEPENDS_ON_PAST: default_args[__DEPENDS_ON_PAST] if __DEPENDS_ON_PAST in default_args else False,
                }

                default_args.update(override_args)

                dag = DAG(
                    dag_id=pipeline_name,
                    default_args=default_args,
                    dagrun_timeout=timedelta(minutes=pipeline['timeout_minutes']),
                    catchup=False
                )

                job_start_task = JobStartTask(dag, pipeline_name, None, pipeline, 'all_success')
                parent = job_start_task.apply_task_to_dag()

                create_stacks_tasks = create_stacks(dag, pipeline, parent)

                if create_stacks_tasks:
                    parent = create_stacks_tasks

                trigger_rule = 'all_success'
                if 'always_run' in config and config['always_run']:
                    trigger_rule = 'all_done'

                for task in pipeline['tasks']:
                    task_type = task['type']
                    task_instance = get_task_class(task_type)(
                        dag, pipeline['pipeline'], parent if parent else None, task, trigger_rule
                    )
                    parent = task_instance.apply_task_to_dag()

                delete_stacks_tasks = delete_stacks(dag, pipeline, parent)

                if delete_stacks_tasks:
                    parent = delete_stacks_tasks

                job_end_task = JobEndTask(dag, pipeline_name, parent, pipeline, 'all_done')
                job_end_task.apply_task_to_dag()

                print(f'{pipeline_name}: {dag.tasks}')

                globals()[pipeline_name] = dag

                dags.append(dag)

            return dags


print(f'Loading task implementations..')

# TODO: add configuration for user tasks package
task_package = 'rainbow/runners/airflow/tasks'
user_task_package = 'TODO: user_tasks_package'

task_classes = class_util.find_subclasses_in_packages([task_package, user_task_package], Task)

print(f'Finished loading task implementations: {task_classes}')


def get_task_class(task_type):
    return task_classes[task_type]


def create_stacks(dag, pipeline, parent):
    """ Generate all required tasks to create stacks"""
    return handle_stacks_by_method(dag, pipeline, parent, method_type='create')


def delete_stacks(dag, pipeline, parent):
    """ Generate all required tasks to delete stacks"""
    return handle_stacks_by_method(dag, pipeline, parent, method_type='delete')


def handle_stacks_by_method(dag, pipeline, parent, method_type):
    """
    Handling all required tasks for given stack_type according to the the given method_type
    :param dag:
    :param pipeline:
    :param parent: parent rainbow task
    :param method_type: available stack operations: 'delete' or 'create'
    :return: None if pipeline['stacks'] is empty/None. else, return ended stacks task
    """
    stacks = pipeline.get('stacks', [])
    if not stacks:
        print(f'stacks list is empty. skipping {method_type} stacks tasks')
        return None

    stack_package = 'rainbow/runners/airflow/tasks/stacks'
    user_stack_task_package = 'TODO: user_stacks_package'
    resources_dict_key = 'resources'

    start_handle_stacks = DummyOperator(dag=dag, task_id=f'start_{method_type}_all_stacks', trigger_rule='all_success')
    parent.set_downstream(start_handle_stacks)
    parent = start_handle_stacks

    end_handle_stacks = DummyOperator(dag=dag, task_id=f'end_{method_type}_all_stacks', trigger_rule='all_success')

    for stack_task in stacks:
        stack_task[resources_dict_key] = pipeline[resources_dict_key]
        stack_task_instance = class_util.get_class_instance([stack_package, user_stack_task_package], StackTask,
                                                            stack_task['type'])(dag, pipeline['pipeline'], parent,
                                                                                stack_task, 'all_done', method_type)
        end_handle_stacks.set_upstream(stack_task_instance.apply_task_to_dag())

    return end_handle_stacks


register_dags(Variable.get('rainbows_dir'))
