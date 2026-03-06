from datetime import date
import json
import logging

logger = logging.getLogger(__name__)

def load_data():
    file_path = f'./data/YTDATA_{date.today()}.json'
    
    try:
        logger.info(f"Processing file: YT_Data-{date.today}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return data
    except FileNotFoundError:
        logger.error(f"File not found:{file_path}")
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file:{file_path}")
        raise
