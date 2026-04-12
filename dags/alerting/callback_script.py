cat > ~/airflow-code-demo/dags/alerting/callback_script.py << 'EOF'
import sys
import boto3
sys.path.append('/home/ubuntu/airflow-code-demo/dags')
from alerting.slack_alert import task_fail_slack_alert

def send_sns_alert(context):
    sns = boto3.client('sns', region_name='us-east-1')
    topic_arn = 'arn:aws:sns:us-east-1:010659611291:airflow-pipeline-alerts'

    task_id = context['task_instance'].task_id
    dag_id  = context['task_instance'].dag_id

    sns.publish(
        TopicArn=topic_arn,
        Subject=f'Airflow FAILURE: {dag_id}',
        Message=f'Task {task_id} in DAG {dag_id} failed.\n'
                f'Execution date: {context["execution_date"]}'
    )
    print('SNS alert sent!')

def callback_function(**context):
    task_fail_slack_alert(**context)
    send_sns_alert(context)
EOF
