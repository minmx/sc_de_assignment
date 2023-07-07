# SC DE Take home assignment
## Part 1
Load data from 4 sensors into a dataframe and perform data processing
1. Sensor 1 - .xlsx file
2. Sensor 2 - .csv file
3. Sensor 4 - .parquet file
4. Sensor 4 different date - .pickle file
5. Sensor 5 - .json file

Data processing steps:
1. Pivot to set timestamp as index column and tag names as columns
2. Filter for "good" category in [quality] column
3. Forward fill values for missing timestamps
4. Scale data to between 0 and 1 for each sensor

## Part 2
Create frontend framework using Dash on Python to display line chart and deploy app on a Docker container

Docker cmd line to build and run:
> docker build -t de_assign .

> docker run -d -p 8080:8080 de_assign
