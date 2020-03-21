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
from flatdict import FlatDict

from rainbow.core.util import class_util
from rainbow.runners.airflow.model import task
from rainbow.runners.airflow.tasks.resources.resource import ResourceTask


class SparkTask(task.Task):
    """
    Executes a Spark application.
    """

    def __init__(self, dag, pipeline_name, parent, config, trigger_rule):
        super().__init__(dag, pipeline_name, parent, config, trigger_rule)
        self.source_path = self.config['source_path']
        self.task_name = self.config['task']
        self.spark_submit = self.__generate_spark_submit()

    def apply_task_to_dag(self):
        resource_config = self.config['resource']

        resource_config.get('parameters', {})['task'] = self.task_name
        resource_task = self.__get_resource_task(resource_config['type'])(
            self.dag, self.pipeline_name, self.parent, resource_config['parameters'], self.trigger_rule,
            self.spark_submit)

        return resource_task.apply_task_to_dag()

    def __generate_spark_submit(self):
        spark_submit = ['spark-submit']

        spark_arguments = self.__spark_args(self.config['spark_arguments'])
        application_arguments = self.__additional_arguments(self.config['application_arguments'])

        spark_submit.extend(spark_arguments)
        spark_submit.extend([self.source_path])
        spark_submit.extend(application_arguments)

        return spark_submit

    @classmethod
    def __get_resource_task(cls, resource_task_type):
        resource_task_package = 'rainbow/runners/airflow/tasks/resources'
        return class_util.get_class_instance([resource_task_package, 'TODO'], ResourceTask, resource_task_type)

    @classmethod
    def __spark_args(cls, params: dict):
        # reformat spark conf
        params['conf'] = ', '.join(['{}={}'.format(k, v) for (k, v) in FlatDict(params['conf']).items()])
        return cls.__from_dict_to_list(cls.__reformat_dict_keys(params))

    @classmethod
    def __additional_arguments(cls, params: dict):
        return cls.__from_dict_to_list(cls.__reformat_dict_keys(params))

    @classmethod
    def __reformat_dict_keys(cls, params):
        return {"--{}".format(x): y for (x, y) in FlatDict(params).items()}

    @classmethod
    def __from_dict_to_list(cls, dictionary):
        steps = []
        for k, v in dictionary.items():
            steps.append(k)
            steps.append(v)

        return steps
