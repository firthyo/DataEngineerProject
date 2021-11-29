import json
from datetime import datetime

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import requests
# ethics commission

def get_ethics_commission_report():
    url = 'https://data.sfgov.org/resource/pv99-gzft.json'
    response = requests.get(url)
    dataSet = response.json()
    # with open('data.json', 'w') as f:
    #     json.dump(data, f)

    return dataSet


def save_data_into_db():
    # mysql_hook = MySqlHook(mysql_conn_id='ethics_commission')
    dataSet = get_ethics_commission_report()
    for data in dataSet:
        import mysql.connector
        db = mysql.connector.connect(host='52.23.18.185',user='root',passwd='password',db='ethics_commission')
        cursor = db.cursor()
        docusignid = data['docusignid'].replace('\n',' ')
        publicurlRaw = data['publicurl']
        publicurl = publicurlRaw['url'].replace('\n',' ')
        filingtype = data['filingtype'].replace('\n',' ')
        cityagencyname = data['cityagencyname'].replace('\n',' ')
        cityagencycontactname = data['cityagencycontactname'].replace('\n',' ')
        natureofcontract = data['natureofcontract'].replace('\n',' ')
        datesigned = data['datesigned'].replace('\n',' ')
        cursor.execute('INSERT INTO ethics_commission_reports (docusignid,publicurl, filingtype,cityagencyname,cityagencycontactname, natureofcontract, datesigned )'
                                'VALUES("%s","%s", "%s", "%s","%s", "%s", "%s")',
                                (docusignid,publicurl, filingtype,cityagencyname,cityagencycontactname, natureofcontract, datesigned))
        db.commit()
        print("Record inserted successfully into testcsv table")
        cursor.close()



default_args = {
    'owner': 'siriyaporn',
    'start_date': datetime(2021, 11, 27),
    'email': ['63606049@kmitl.ac.th'],
}
with DAG('sf_ethics_commission_report',
         schedule_interval='@daily',
         default_args=default_args,
         description='ethics commission report',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='ethics_commission_report',
        python_callable=get_ethics_commission_report
    )

    t2 = PythonOperator(
        task_id='save_data_into_db',
        python_callable=save_data_into_db
    )

    

    t1 >> t2 