from airflow.utils.email import send_email

def callback_function(context):
    print("Task failed. Callback triggered.")

    # Optional: Slack or email logic later
