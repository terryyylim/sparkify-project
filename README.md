## Introduction
This project involves data modeling with Postgres and building of a simple ETL pipeline using Python. The purpose of these Postgres tables would be to analyze songs and user activity for startup, Sparkify - Music Streaming Platform.

Since there is a need to analyze the data collected, the information currently stored in the JSON files should be indexed into a relational database to allow for the ability to do aggregations and analytics. Using the STAR schema approach, the tables would be denormalized and queries will be simpler to write, while allowing for faster aggregations.

The STAR schema consist of the following fact and dimension tables.

#### Fact Table
`songplays`

#### Dimensions Table
`users`, `songs`, `artists`, `time`

## Installation
If you're running this code locally, remember to have Postgres setup in your system.

Clone this repository:
```
git clone https://github.com/terryyylim/sparkify-project.git
```

Change to sparkify directory
```
cd sparkify
```

To prevent complications to the global environment variables, I suggest creating a virtual environment for tracking and using the libraries required for the project.

1. Create and activate a virtual environment (run `pip3 install virtualenv` first if you don't have Python virtualenv installed):
```
virtualenv -p `which python3.6` venv
source venv/bin/activate
```

3. Install the requirements:
```
pip install -r requirements.txt
```

## What do each file do
1. create_tables.py
- Contains helper functions which connects to the database (sparkifydb) and creates the necessary tables for insertion of data
2. etl.py
- Contains helper function which connects to the database (sparkifydb) and inserts the data from the JSON files into the SQL tables
3. sql_queries.py
- Contains schemas for the tables to be created when running create_tables.py
- Contains insertion, deletion and select queries for the music streaming data stored in the JSON files
4. etl.ipynb
- Allows step-by-step formation of the ETL pipeline before refactoring the code into etl.py script
5. test.ipynb
- Contains code to check if the database (sparkifydb) has been created successfully and if changes to database was done correctly

### Quick Run
To test if the schema is set up correctly, run the following files in order. There should be no error being surfaced.
```
python create_tables.py
python etl.py
```
