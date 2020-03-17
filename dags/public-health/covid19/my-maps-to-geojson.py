from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 16),
    "email": ["ian.rose@lacity.org", "hunter.owens@lacity.org", "itadata@lacity.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(hours=1),
}

dag = DAG(
    'lol',
    default_args=default_args,
    description='Convert LAUSD myMap to Geojson',
    schedule_interval=timedelta(days=1),
)

templated_command = """
rm /tmp/lausd-map.kml 
rm /tmp/lausd-map.geojson
wget -O /tmp/lausd-map.kml https://www.google.com/maps/d/u/0/kml?mid=1_R_MQhVYaKh3A5_8oAtOtlNK2XWjzP2t&nl=1
ogr2ogr -f "GeoJSON" /tmp/lausd-map.geojson /tmp/lausd-map.kml 
"""

t1 = BashOperator(
    task_id='download',
    depends_on_past=False,
    bash_command="echo 3",
    dag=dag,
)