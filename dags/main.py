from airflow import DAG
import pendulum
from datetime import datetime, timedelta

from api.video_stats import get_playlist_id, get_video_ids, get_video_stats, save_to_json

# Define local timezone
local_tz = pendulum.timezone("Asia/Kuala_Lumpur")

# Default Args
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # 'end_date': datetime(2030, 12, 31, tzinfo=local_tz),
}

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='DAG to produce JSON file with raw data',
    schedule='0 14 * * *', # crontab. guru
    catchup=False
) as dag:
    
    # Define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_stats = get_video_stats(video_ids)
    save_to_json_task = save_to_json(video_stats)
    
    # Define dependencies
    
    playlist_id >> video_ids >> video_stats >> save_to_json_task
    