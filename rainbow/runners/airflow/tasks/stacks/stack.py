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

from rainbow.runners.airflow.model import task


class StackTask(task.Task):
    """
    Stack task
    """
    def __init__(self, dag, pipeline_name, parent, config, trigger_rule, method):
        super().__init__(dag, pipeline_name, parent, config, trigger_rule)
        self.method = method
        print(self.config)
        self.stack_name = self.config['name']

    def apply_task_to_dag(self):
        return getattr(self, self.method)()

    @abstractmethod
    def create(self):
        pass

    @abstractmethod
    def delete(self):
        pass
