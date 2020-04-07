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
from rainbow.runners.airflow.tasks.clusters.cluster import ClusterTask

from rainbow.core.util import class_util, rainbow_util
from rainbow.runners.airflow.model import task


class SparkTask(task.Task):
    """
    Executes a Spark application.
    """

    def __init__(self, dag, pipeline_name, parent, config, trigger_rule):
        super().__init__(dag, pipeline_name, parent, config, trigger_rule)
        self.source_path = self.config['source_path']
        self.task_name = self.config['task']
        self.cluster_config = self.config['resources'][self.config['stack_id']]
        self.spark_submit = self.__generate_spark_submit()

    def apply_task_to_dag(self):
        cluster_params = self.cluster_config.get('parameters', {})
        cluster_params['task'] = self.task_name
        cluster_task = self.__get_cluster_task(self.cluster_config['cluster_type'])(
            self.dag, self.pipeline_name, self.parent, cluster_params, self.trigger_rule,
            self.spark_submit)

        return cluster_task.apply_task_to_dag()

    def __generate_spark_submit(self):
        spark_submit = ['spark-submit']

        spark_arguments = self.__spark_args(self.config['spark_arguments'])
        application_arguments = self.__additional_arguments(self.config['application_arguments'])

        spark_submit.extend(spark_arguments)
        spark_submit.extend([self.source_path])
        spark_submit.extend(application_arguments)

        return spark_submit

    @classmethod
    def __get_cluster_task(cls, cluster_task_type):
        clusters_package = 'rainbow/runners/airflow/tasks/clusters'
        return class_util.get_class_instance([clusters_package, 'TODO'], ClusterTask,
                                             cluster_task_type)

    @classmethod
    def __spark_args(cls, params: dict):
        # reformat spark conf
        conf_args_list = list()

        for conf_arg in ['{}={}'.format(k, v) for (k, v) in FlatDict(params['conf']).items()]:
            conf_args_list.append('--conf')
            conf_args_list.append(conf_arg)

        params.__delitem__('conf')

        spark_arguments = rainbow_util.from_dict_to_list(rainbow_util.reformat_dict_keys(params, "--{}"))
        spark_arguments.extend(conf_args_list)
        return spark_arguments

    @classmethod
    def __additional_arguments(cls, params: dict):
        return rainbow_util.from_dict_to_list(params)
