from datetime import timedelta, datetime

def parse_duration(duration_str) -> timedelta:
    
    #P1DT2H30M45S -> 1D2H30M45S
    duration_str = duration_str.replace("P", "").replace("T", "") 
    
    components = ['D', 'H', 'M', 'S'] #day, hour, minute, second
    values = {
        "D": 0,
        "H": 0,
        "M": 0,
        "S": 0
    }
    
    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component)
            values[component] = int(value)
    
    total_duration = timedelta(
        days = values["D"],
        hours = values["H"],
        minutes = values["M"],
        seconds = values["S"]
    )
    
    return total_duration

def transform_data(row):
    
    duration_td = parse_duration(row['Duration'])
    
    # datetime.min here is datetime(1, 1, 1, 0, 0, 0)
    # + duration_td -> datetime(1, 1, 1, 2, 30, 45)
    # time() -> time(2, 30, 45)
    row['Duration'] = (datetime.min + duration_td).time()
    
    row["Video_Type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal"
    
    return row