import time

def cal_time_taken(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        
        result = func(*args, **kwargs)
        
        end_time = time.time()
        
        time_taken = end_time - start_time
        
        print(f'Time taken to process the function {func.__name__}: {round(time_taken, 2)} seconds')
        print(" ")
    
        return result
    
    return wrapper 
        
        