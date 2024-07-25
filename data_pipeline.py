import pandas as pd

# Step-by-step ETL Process 
# Extract data from source and load to datalake as parquet file
# Extract parquet file in datalake, load as dimensional and fact table to schema datawarehouse_l0 (I'll load the table as csv file for simplycity)
# Extract table in schema datawarehouse_l0, do transform and load to schema datawarehouse_l1 (I'll load the table as csv file for simplycity)
# Extract table in schema datawarehouse_l1, do transform and load to schema datamart (I'll load the table as csv file for simplycity)


# Datalake
def csv_to_parquet(tables, engine='pyarrow'):
    for table_name in tables:
        csv_file_path = f'/Users/dilla/Downloads/agriaku/myenv/directories/source/{table_name}.csv'
        parquet_file_path = f'/Users/dilla/Downloads/agriaku/myenv/directories/datalake/{table_name}.parquet'
        df = pd.read_csv(csv_file_path)
        df.columns = df.columns.str.lower()
        df.to_parquet(parquet_file_path, engine=engine, index=False)


# Datawarehouse layer 0
def read_parquet(table_name, engine='pyarrow'):
    parquet_file_path = f'/Users/dilla/Downloads/agriaku/myenv/directories/datalake/{table_name}.parquet'
    df = pd.read_parquet(parquet_file_path, engine=engine)
    return df

def read_csv(table_name,schema):
    csv_file_path = f'/Users/dilla/Downloads/agriaku/myenv/directories/{schema}/{table_name}.csv'
    with open(csv_file_path, 'r') as csvfile:
        df = pd.read_csv(csvfile)
    return df

def generate_semester_weeks(start_date, end_date, semester):
    date_range = pd.date_range(start=start_date, end=end_date)
    weeks = (date_range - pd.to_datetime(start_date)).days // 7 + 1
    df = pd.DataFrame({'date': date_range, 'semester': semester, 'week': weeks})
    return df

def table_l0(tables):
    for table_name in tables:
        df = read_parquet(table_name)
        df = df.rename(columns={'id':f'{table_name}_id'})
        csv_string = df.to_csv(index=False)
        with open(f'/Users/dilla/Downloads/agriaku/myenv/directories/datawarehouse_l0/{table_name}.csv', 'w', newline='') as file:
                    file.write(csv_string)

def dim_fact_table_l1(tables):
    # dim table from source
    for table_name in tables:
        df = read_csv(table_name,schema='datawarehouse_l0')
        df = df.rename(columns={'id':f'{table_name}_id'})
        csv_string = df.to_csv(index=False)
        with open(f'/Users/dilla/Downloads/agriaku/myenv/directories/datawarehouse_l1/dim_{table_name}.csv', 'w', newline='') as file:
                    file.write(csv_string)

    # new dim table
    enrollment = read_parquet(table_name='enrollment')
    schedule = read_parquet(table_name='schedule')
    course_attendance = read_parquet(table_name='course_attendance')
    course = read_parquet(table_name='course')
    schedule_enroll = pd.merge(enrollment, schedule, how='left', left_on='schedule_id', right_on='id')
    start_date = schedule_enroll['start_dt'].unique().tolist()
    end_date = schedule_enroll['end_dt'].unique().tolist()
    semester = schedule_enroll['semester'].unique().tolist()

    all_dates = pd.DataFrame()

    for start, end, sem in zip(start_date, end_date, semester):
        df = generate_semester_weeks(start, end, sem)
        all_dates = pd.concat([all_dates, df])

    all_dates = all_dates.reset_index().rename(columns={'index':'week_sk_id'})
    csv_string = all_dates.to_csv(index=False)
    with open(f'/Users/dilla/Downloads/agriaku/myenv/directories/datawarehouse_l1/dim_week.csv', 'w', newline='') as file:
                file.write(csv_string)

    # fact table
    course_attendance['attend_dt'] = pd.to_datetime(course_attendance['attend_dt'], format='%d-%b-%y')
    course_attendance['attend_dt'] = course_attendance['attend_dt'].dt.strftime('%Y-%m-%d')
    all_dates['date'] = all_dates['date'].dt.strftime('%Y-%m-%d')
    t1 = pd.merge(enrollment, schedule, how='left', left_on='schedule_id', right_on='id', suffixes=('', '_schedule')).merge(course, how='left', left_on='course_id', right_on='id', suffixes=('', '_course')).merge(course_attendance, how='left', on=['student_id','schedule_id'], suffixes=('', '_attendance')).merge(all_dates, how='left', left_on='attend_dt',right_on='date')
    t1 = t1.rename(columns={'id':'enrollment_id', 'id_attendance':'course_attendance_id'})
    fact = t1[['course_id', 'enrollment_id', 'course_attendance_id', 'schedule_id','week_sk_id','attend_dt']]
    csv_string = fact.to_csv(index=False)
    with open(f'/Users/dilla/Downloads/agriaku/myenv/directories/datawarehouse_l1/fact_program_attendance.csv', 'w', newline='') as file:
                file.write(csv_string)


def mart_table():
    dim_enrollment = read_csv(table_name='dim_enrollment',schema='datawarehouse_l1')
    dim_schedule = read_csv(table_name='dim_schedule',schema='datawarehouse_l1')
    dim_week = read_csv(table_name='dim_week',schema='datawarehouse_l1')
    dim_course = read_csv(table_name='dim_course',schema='datawarehouse_l1')
    dim_course_attendance = read_csv(table_name='dim_course_attendance',schema='datawarehouse_l1')
    fact_table = read_csv(table_name='fact_program_attendance',schema='datawarehouse_l1')
    t2 = pd.merge(fact_table, dim_schedule, how='left', on='schedule_id').merge(dim_enrollment, how='left', on='enrollment_id').merge(dim_course, how='left', left_on='course_id_x', right_on='course_id').merge(dim_course_attendance, how='left', on='course_attendance_id').merge(dim_week, how='left', left_on=['week_sk_id','semester'], right_on=['week_sk_id','semester'])
    mart = t2[['semester','week', 'name', 'schedule_id']]
    grouped_counts = mart.groupby(['semester', 'week',  'name']).size().reset_index(name='total_attendance')

    grouped_counts.columns = ['semester_id','week_id', 'course_name','amount']
    semester_totals = mart['semester'].value_counts().reset_index()
    semester_totals.columns = ['semester_id', 'total_amount']

    result = pd.merge(grouped_counts, semester_totals, on='semester_id')
    result['attendance_pct'] = result['amount']/result['total_amount']*100
    final_result = result[['semester_id', 'week_id', 'course_name', 'attendance_pct']]

    csv_string = final_result.to_csv(index=False)
    with open(f'/Users/dilla/Downloads/agriaku/myenv/directories/datamart/weekly_report_attendance.csv', 'w', newline='') as file:
                file.write(csv_string)

if __name__ == "__main__":
    tables = ['enrollment', 'course', 'course_attendance', 'schedule']
    #datalake
    csv_to_parquet(tables, engine='pyarrow')
    print('Done load to datalake')

    #datawarehouse_l0
    table_l0(tables)
    print('Done load to datawarehouse_l0')

    #datawarehouse_l1
    dim_fact_table_l1(tables)
    print('Done load to datawarehouse_l1')

    # datamart
    mart_table()
    print('Done load to datamart')