# university_pipeline
Data pipeline from source to datamart, about data course, enrollment, schedule in university.
Source data consist of 4 csv files;
- enrollment.csv
- schedule.csv
- course.csv
- course_attendance.csv
I create data pipeline using python. Idealy I will load the data to RDBMS, but for simplicity in this case, I load the data as files in directory attached.
Each schema and layer database reflected in folder directories and the table result of each process reflected as csv files.

directories
| 
| ---- datalake
| ---- datamart
| ---- datawarehouse_l0
| ---- datawarehouse_l1
| ---- source

How to run the code:
1. install python
2. activate virtual environment
3. run the script from terminal
`python3 data_pipeline.py`
