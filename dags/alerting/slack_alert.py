from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

SLACK_CONN_ID = 'Slack_Connection'

def task_fail_slack_alert(context):
    slack_token = BaseHook.get_connection(SLACK_CONN_ID).password
    task_id  = context['task_instance'].task_id
    dag_id   = context['task_instance'].dag_id
    exec_dt  = context['execution_date']

    msg = (
        f":red_circle: *Pipeline Failed!*\n"
        f"*DAG:* {dag_id}\n"
        f"*Task:* {task_id}\n"
        f"*Execution Time:* {exec_dt}"
    )

    alert = SlackWebhookOperator(
        task_id='slack_fail_alert',
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=msg,
    )
    alert.execute(context=context)

def task_success_slack_alert(**context):
    dag_id  = context['task_instance'].dag_id
    exec_dt = context['execution_date']

    msg = (
        f":large_green_circle: *Pipeline Completed Successfully!*\n"
        f"*DAG:* {dag_id}\n"
        f"*Execution Time:* {exec_dt}"
    )

    alert = SlackWebhookOperator(
        task_id='slack_success_alert',
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=msg,
    )
    alert.execute(context=context)
