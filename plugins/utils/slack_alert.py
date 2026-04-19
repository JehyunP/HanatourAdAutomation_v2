from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from configs.logging_config import SLACK_CONN_ID


def on_success(context):
    ti = context["task_instance"]
    sections = []
    
    errors = ti.xcom_pull(key="return_value") or {}
    
    duplicated = ti.xcom_pull(
        task_ids="create_ep_task_id",
        key="removed_product_codes"
    ) or []
    
    status_check_error = errors.get("status_check_error", [])
    product_disable = errors.get("product_disable", [])
    no_seat = errors.get("no_seat", [])

    sections = [
        f"- 중복 상품: {len(duplicated)}건",
        f"- 상태 조회 실패: {len(status_check_error)}건",
        f"- 가격 조회 실패: {len(product_disable)}건",
        f"- 제외 상품: {len(no_seat)}건",
    ]

    detail_message = "\n".join(sections)

    message = f"""
:white_check_mark: Task Success

DAG: `{ti.dag_id}`
Task: `{ti.task_id}`
Run ID: `{context.get('run_id')}`
Execution Date: `{context.get('logical_date')}`

{detail_message}
"""

    hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)
    hook.send(text=message)







def on_failure(context):
    ti = context["task_instance"]
    exception = context.get("exception")

    errors = ti.xcom_pull(key="return_value") or {}

    status_check_error = errors.get("status_check_error", [])
    product_disable = errors.get("product_disable", [])
    no_seat = errors.get("no_seat", [])
    
    duplicated = ti.xcom_pull(
        task_ids="create_ep_task_id",
        key="removed_product_codes"
    ) or []

    sections = [
        f"- 중복 상품: {len(duplicated)}건",
        f"- 상태 조회 실패: {len(status_check_error)}건",
        f"- 가격 조회 실패: {len(product_disable)}건",
        f"- 제외 상품: {len(no_seat)}건",
    ]
    detail_message = "\n".join(sections)
    
    message = f"""
:x: Task Failed

DAG: `{ti.dag_id}`
Task: `{ti.task_id}`
Run ID: `{context.get('run_id')}`
Execution Date: `{context.get('logical_date')}`

Exception:
`{exception}`

{detail_message}
"""

    hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)
    hook.send(text=message)