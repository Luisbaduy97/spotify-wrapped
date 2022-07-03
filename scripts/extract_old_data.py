import pandas as pd
import time
import datetime
today = datetime.date.today()
start_date = datetime.datetime(
    year=today.year,
    month=1,
    day=1
)
end_date = datetime.datetime(
     year=today.year,
    month=today.month,
    day=today.day
)
date_generated = [start_date + datetime.timedelta(days=x) for x in range(0, (end_date-start_date).days)]
print(date_generated) 