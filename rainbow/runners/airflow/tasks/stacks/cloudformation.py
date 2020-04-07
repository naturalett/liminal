from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from flatdict import FlatDict

from rainbow.runners.airflow.operators.cloudformation import CloudFormationCreateStackOperator, \
    CloudFormationCreateStackSensor, CloudFormationHook, CloudFormationDeleteStackOperator, \
    CloudFormationDeleteStackSensor
from rainbow.runners.airflow.tasks.stacks.stack import StackTask


class CloudFormationStackTask(StackTask):
    """
    Cloudformation stack task
    """
    def __init__(self, dag, pipeline_name, parent, config, trigger_rule, method):
        super().__init__(dag, pipeline_name, parent, config, trigger_rule, method)

    def create(self):
        return CreateCloudFormationStackTask(self.dag, self.pipeline_name, self.parent, self.config, self.trigger_rule) \
            .apply_task_to_dag()

    def delete(self):
        return DeleteCloudFormationStackTask(self.dag, self.pipeline_name, self.parent, self.config, self.trigger_rule) \
            .apply_task_to_dag()


class CreateCloudFormationStackTask(CloudFormationStackTask):
    """
    Creates Cloudformation stack.
    """

    def __init__(self, dag, pipeline_name, parent, config, trigger_rule):
        super().__init__(dag, pipeline_name, parent, config, trigger_rule, method='')
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

        return stack_creation_end_task

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


class DeleteCloudFormationStackTask(CloudFormationStackTask):
    """
    Deletes Cloudformation stack.
    """

    def __init__(self, dag, pipeline_name, parent, config, trigger_rule):
        super().__init__(dag, pipeline_name, parent, config, trigger_rule, method='')

    def apply_task_to_dag(self):
        check_dags_queued_task = BranchPythonOperator(
            task_id='is_dag_queue_empty',
            python_callable=self.__queued_dag_runs_exists,
            provide_context=True,
            trigger_rule=self.trigger_rule,
            dag=self.dag
        )

        delete_stack_task = CloudFormationDeleteStackOperator(
            task_id='delete_cloudformation_stack',
            params={'StackName': self.stack_name},
            dag=self.dag
        )

        delete_stack_sensor = CloudFormationDeleteStackSensor(
            task_id='cloudformation_watch_cluster_delete',
            stack_name=self.stack_name,
            dag=self.dag
        )

        stack_delete_end_task = DummyOperator(
            task_id='stack_delete_end',
            dag=self.dag
        )

        if self.parent:
            self.parent.set_down_sream(check_dags_queued_task)

        check_dags_queued_task.set_downstream(stack_delete_end_task)
        check_dags_queued_task.set_downstream(delete_stack_task)
        delete_stack_task.set_downstream(delete_stack_sensor)
        delete_stack_sensor.set_downstream(stack_delete_end_task)

        return stack_delete_end_task

    def __queued_dag_runs_exists(self, **kwargs):
        if self.dag.get_num_active_runs() > 1:
            return 'stack_delete_end'
        else:
            return 'delete_cloudformation_stack'
