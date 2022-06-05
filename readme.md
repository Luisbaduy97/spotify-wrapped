# Spotify wrapped

This project extracts the songs that the user listened to on spotify and calculates the minutes listened to per song and per artist. Airflow was used to automate the extraction and transformation of data from the spotify api.

## Tools and frameworks used

1. Python
2. Airflow
3. Pandas
4. SQL

## Setup
```bash
# Create virtual env
virtualenv venv
# activate environment
source venv/bin/activate
# install airflow
sh airflow.sh
# install requirements
pip install -r requirements
# start postgres db
sh scripts/set_db.sh
```

This project was made with :heart:  by <a href='https://www.linkedin.com/in/luis-navarrete-baduy-53bb30176/'>Luis Navarrete</a>.