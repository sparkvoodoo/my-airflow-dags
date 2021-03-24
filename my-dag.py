
import time
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import json
import requests
from requests.auth import  AuthBase
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 11, 1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "brandon.parker@inmoment.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

rap_status = {
    'P': 'Passed',
    'F': 'Failed',
    'I': 'In-progress',
    'D': 'Delete queued',
    'Q': 'Queued',
    'L': 'Launching new cluster',
    'W': 'Waiting in workflow queue',
    'Z': 'Zero record input'
}


# url should be an airflow env variable
url = 'https://api.rap.demo.inmoment.com'
# token should be retrieved from Auth0.com instead of hardcoded
token = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InVpdU04MDE5NjdqYW9neGFKUWl0cSJ9.eyJnaXZlbl9uYW1lIjoiQnJhbmRvbiIsImZhbWlseV9uYW1lIjoiUGFya2VyIiwibmlja25hbWUiOiJicmFuZG9uLnBhcmtlciIsIm5hbWUiOiJCcmFuZG9uIFBhcmtlciIsInBpY3R1cmUiOiJodHRwczovL2xoMy5nb29nbGV1c2VyY29udGVudC5jb20vYS0vQU9oMTRHZ0lmVlZSZFpFOXh5OHpNMkxnQ096dTZ6MWlLUXBEYXdvUjVWdjg9czk2LWMiLCJsb2NhbGUiOiJlbiIsInVwZGF0ZWRfYXQiOiIyMDIxLTAzLTIyVDE2OjEyOjU1LjYyMFoiLCJpc3MiOiJodHRwczovL2lubW9tZW50LXJhcC1kZW1vLnVzLmF1dGgwLmNvbS8iLCJzdWIiOiJnb29nbGUtb2F1dGgyfDExNzMwMjM0OTk3NDk4MzAwMzM4NyIsImF1ZCI6InlmcHdkMVhvTDVoVHZBUGJBc2tvVk5JekFHSUJBMzlQIiwiaWF0IjoxNjE2NDI5NTc1LCJleHAiOjE2MTY0NjU1NzUsImF0X2hhc2giOiJXUERPYkJteVVOS0hrTk5xUlVpSlh3Iiwibm9uY2UiOiI5akFKRWZhUHhSRDNDbC1xNW9GcEgzanVHRmNGWmZyMSJ9.LEhnSFH5bzN1Vdar8Gbqv0LvHQwysYCrX-FxqUZxI2gWrWgA0nXn4Nxqqs_3GVM_P_bm_Gut8gW26r4SXdvBfrw8wl76g5kMJkUrrDm5UAHYQ7EVJ_wj48i6UHWE09nrZ_zWEZlPUPUrT_bEI5MPQuq0YequjxyTLBjaQiWI-D2_cTB_DSEp2Cq1Kbbb-iYiNMgDWtMKdcI1sm7mO9FjmMAHqZR9HcrgSUJ11C64SCiySn5BGYWp_sg9uQRkqOyNpYDnemqXaKlMJPDwJb3VtDfrNjVRY9ZURlxrUIdZu9n3mL12NMRY3XqmE8Qw62yz1cSp-0gjYxdcIkF1VDCKDg'

class TokenAuth(AuthBase):
    """Implements a custom authentication scheme."""

    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        """Attach an API token to a custom auth header."""
        r.headers['authorization'] = f'{self.token}'  # Python 3.6+
        return r



def get_status(base_url, auth_token, source_id):
    try:
        data = '{{"source_id":{value}, "limit":1, "effective_filter":false}}'.format(value=source_id)
        response = requests.post(url=base_url + '/inputs-filtered',
                                 auth=TokenAuth(auth_token),
                                 data=data,
                                 headers={'Content-Type': 'application/json'})

        if response.status_code == 200:
            inputs = response.json()
            pull_status_code = list(inputs.values())[0][0]["status_code"]
            print(pull_status_code)
            return pull_status_code

    except Exception as e:
        print(e)
    return None


def pull_source(base_url, auth_token, source_id):
    try:
        response = requests.put(base_url + '/source-pull/' + str(source_id), auth=TokenAuth(auth_token))
        print(response.json())
        return 1
    except Exception as e:
        print(e)
        return e


def pull_source_sync(base_url, auth_token, source_id):
    # run the command to pull the source data
    pull_source(base_url, auth_token, source_id)
    # loop until we get a non complete status
    while True:
        status = get_status(base_url, auth_token, source_id)
        print(status)
        if status not in ('I', 'Q', 'L', 'W'):
            return status
        time.sleep(5)


dag = DAG(
    'data-migration',
    default_args=default_args,
    description='Data migration source pull DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['data-migration'],
)

t1 = PythonOperator(
    task_id='pull_source',
    python_callable=pull_source_sync,
    op_kwargs={base_url: url, auth_token: token, source_id: 501},
    dag=dag,
)

