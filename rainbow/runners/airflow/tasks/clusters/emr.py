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

from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

from rainbow.runners.airflow.tasks.clusters import cluster


class EMRClusterTask(cluster.ClusterTask):
    """
    Emr executable resource task
    """

    def __init__(self, dag, pipeline_name, parent, config, trigger_rule, executable_commands):
        super().__init__(dag, pipeline_name, parent, config, trigger_rule, executable_commands)
        self.aws_conn_id = self.config['aws_conn_id']
        self.cluster_states = self.config['cluster_states']
        self.task_name = self.config['task']
        self.cluster_name = self.config['cluster_name']

    def apply_task_to_dag(self):
        from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
        add_step = EmrAddStepsOperator(
            task_id=f'{self.task_name}_add_step',
            job_flow_name=self.cluster_name,
            aws_conn_id=self.aws_conn_id,
            steps=self.command,
            cluster_states=self.cluster_states,
            dag=self.dag
        )

        if self.parent:
            self.parent.set_downstream(add_step)

        emr_sensor_step = EmrStepSensor(
            task_id=f'{self.task_name}_watch_step',
            job_flow_id=f'''{{ task_instance.xcom_pull({add_step.task_id}, key='job_flow_id') }}''',
            step_id=f'''{{ task_instance.xcom_pull({add_step.task_id}, key='return_value')[0] }}''',
            aws_conn_id=self.aws_conn_id,
            dag=self.dag
        )

        add_step.set_downstream(emr_sensor_step)

        return emr_sensor_step
