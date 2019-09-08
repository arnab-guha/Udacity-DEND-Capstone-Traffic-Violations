class etl:
    """This class ha sthe variables which contains SQL and python codes necessary to load the fact and dimension tables"""
    #date load sql
    insert_dim_date = """
        INSERT INTO dim_date
        SELECT TO_CHAR(datum,'yyyymmdd')::INT AS id_date,
        datum AS date_actual,
        TO_CHAR(datum,'Day') AS day_name,
        CAST(EXTRACT(isodow FROM datum) AS INT) AS day_of_week,
        CAST(EXTRACT(DAY FROM datum) AS INT) AS day_of_month,
        CASE WHEN DQ.datum = CURRENT_DATE THEN 1 ELSE 0 END AS current_date_flag,
        CASE WHEN DQ.datum = CURRENT_DATE - 1 THEN 1 ELSE 0 END AS previous_date_flag,
        CASE WHEN DQ.datum = CURRENT_DATE + 1 THEN 1 ELSE 0 END AS next_date_flag,
        TO_CHAR(datum,'W')::INT AS week_of_month,
        CAST(EXTRACT(week FROM datum) AS INTEGER) AS week_of_year,
        CAST(EXTRACT(MONTH FROM datum) AS INTEGER) AS month_number,
        TO_CHAR(datum,'Month') AS month_name,
        TO_CHAR(datum,'Mon') AS month_name_abbr,
        CAST(EXTRACT(quarter FROM datum) AS INTEGER) AS quarter_actual,
        CASE
            WHEN EXTRACT(quarter FROM datum) = 1 THEN 'First'
            WHEN EXTRACT(quarter FROM datum) = 2 THEN 'Second'
            WHEN EXTRACT(quarter FROM datum) = 3 THEN 'Third'
            WHEN EXTRACT(quarter FROM datum) = 4 THEN 'Fourth'
        END AS quarter_name,
        CAST(EXTRACT(isoyear FROM datum) AS INTEGER) AS year_actual,
        datum +(1 -EXTRACT(isodow FROM datum))::INT AS first_day_of_week,
        datum +(7 -EXTRACT(isodow FROM datum))::INT AS last_day_of_week,
        datum +(1 -EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
        (DATE_TRUNC('MONTH',datum) +INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
        DATE_TRUNC('quarter',datum)::DATE AS first_day_of_quarter,
        (DATE_TRUNC('quarter',datum) +INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
        TO_DATE(EXTRACT(isoyear FROM datum) || '-01-01','YYYY-MM-DD') AS first_day_of_year,
        TO_DATE(EXTRACT(isoyear FROM datum) || '-12-31','YYYY-MM-DD') AS last_day_of_year,
        CASE
            WHEN EXTRACT(isodow FROM datum) IN (6,7) THEN TRUE
            ELSE FALSE
        END AS weekend_indweekend_
        FROM (
            SELECT '1970-01-01'::DATE+ SEQUENCE.DAY AS datum
            FROM GENERATE_SERIES (0,29219) AS SEQUENCE (DAY)
            GROUP BY SEQUENCE.DAY
        ) DQ
        ORDER BY ID_DATE
    """

    #vehicle_type dataframe
    vehicle_type = """
vehicletype_lkup = pd.DataFrame(df.vehicletype.unique(),columns=['vehicletype']).reset_index()
del vehicletype_lkup['index']
vehicletype_lkup['vehicletype_id'] = vehicletype_lkup.index+1
vehicletype_lkup[['vehicletype_id','vehicletype']].to_sql(self.table,self.engine,if_exists="append",chunksize=5000,index=False,method='multi')
    """

    #driver dataframe
    driver = """
driver_lkup = df[['gender','race','driver_city','driver_state','driver_dl_state']].drop_duplicates().reset_index()
del driver_lkup['index']
driver_lkup['driver_id'] = driver_lkup.index+1
driver_lkup[['driver_id','gender','race','driver_city','driver_state','driver_dl_state']].to_sql(self.table,self.engine,if_exists="append",chunksize=5000,index=False,method='multi')
    """

    #arret_type dataframe
    arrest_type = """
arrest_type_lkup = df['arrest_type'].drop_duplicates().reset_index()
del arrest_type_lkup['index']
arrest_type_lkup['arrest_type_id'] = arrest_type_lkup.index+1
arrest_type_lkup[['arrest_type_id','arrest_type']].to_sql(self.table,self.engine,if_exists="append",chunksize=5000,index=False,method='multi')
    """

    #violation_type dataframe
    violation_type = """
violation_type_lkup = df['violation_type'].drop_duplicates().reset_index()
del violation_type_lkup['index']
violation_type_lkup['violation_type_id'] = violation_type_lkup.index+1
violation_type_lkup[['violation_type_id','violation_type']].to_sql(self.table,self.engine,if_exists="append",chunksize=5000,index=False,method='multi')
    """

    #subagency dataframe
    subagency = """
subagency_lkup = df[['agency','subagency']].drop_duplicates().reset_index()
del subagency_lkup['index']
subagency_lkup['subagency_id'] = subagency_lkup.index+1
subagency_lkup[['subagency_id','subagency','agency']].to_sql(self.table,self.engine,if_exists="append",chunksize=5000,index=False,method='multi')
    """

    #fact_traffic_violations dataframe
    fact_traffic_violations = """
self.cur.execute("Select * FROM dim_date")
date_lkup = pd.DataFrame(self.cur.fetchall())
colnames = [desc[0] for desc in self.cur.description]
date_lkup.columns = colnames
date_lkup['date_of_stop'] = pd.to_datetime(date_lkup['date_actual'], format='%Y-%m-%d').dt.strftime('%m/%d/%Y')


vehicletype_lkup = pd.DataFrame(df.vehicletype.unique(),columns=['vehicletype']).reset_index()
del vehicletype_lkup['index']
vehicletype_lkup['vehicletype_id'] = vehicletype_lkup.index+1

driver_lkup = df[['gender','race','driver_city','driver_state','driver_dl_state']].drop_duplicates().reset_index()
del driver_lkup['index']
driver_lkup['driver_id'] = driver_lkup.index+1

arrest_type_lkup = df['arrest_type'].drop_duplicates().reset_index()
del arrest_type_lkup['index']
arrest_type_lkup['arrest_type_id'] = arrest_type_lkup.index+1

violation_type_lkup = df['violation_type'].drop_duplicates().reset_index()
del violation_type_lkup['index']
violation_type_lkup['violation_type_id'] = violation_type_lkup.index+1

subagency_lkup = df[['agency','subagency']].drop_duplicates().reset_index()
del subagency_lkup['index']
subagency_lkup['subagency_id'] = subagency_lkup.index+1

df = pd.merge(df,date_lkup,left_on=['date_of_stop'],right_on=['date_of_stop'],how='left')
df = pd.merge(df,vehicletype_lkup,left_on=['vehicletype'],right_on=['vehicletype'],how='left')
df = pd.merge(df,driver_lkup,left_on=['gender','race','driver_city','driver_state','driver_dl_state'],right_on=['gender','race','driver_city','driver_state','driver_dl_state'],how='left')
df = pd.merge(df,arrest_type_lkup,left_on=['arrest_type'],right_on=['arrest_type'],how='left')
df = pd.merge(df,violation_type_lkup,left_on=['violation_type'],right_on=['violation_type'],how='left')
df = pd.merge(df,subagency_lkup,left_on=['agency','subagency'],right_on=['agency','subagency'],how='left')

df = df.dropna().drop_duplicates().reset_index()
del df['index']
df['record_id'] = df.index+1


self.cur.execute("Select * FROM fact_traffic_violations limit 0")
colnames = [desc[0] for desc in self.cur.description]
df = df[colnames]

df.to_csv("/home/arnabguha/Projects/Datasets/fact/fact_traffic.csv", encoding='utf-8', index=False)

code = "COPY fact_traffic_violations FROM STDIN DELIMITER ',' CSV HEADER"
self.cur.copy_expert(code, open("/home/arnabguha/Projects/Datasets/fact/fact_traffic.csv", "r"))
    """

    #fact_traffic_violations_count_agg insert statement
    insert_fact_traffic_violations_count_agg = """
        INSERT INTO fact_traffic_violations_count_agg
        select
        distinct
        a2.id_date,
        a3.subagency_id,
        count(a1.*) violation_count,
        count(case when upper(a1.accident) = 'YES' then 1 end) accidents_count,
        count(case when upper(a1.belts) = 'YES' then 1 end) belts_count,
        count(case when upper(a1.personal_injury) = 'YES' then 1 end) personal_injury_count,
        count(case when upper(a1.property_damage) = 'YES' then 1 end) property_damage_count,
        count(case when upper(a1.fatal) = 'YES' then 1 end) fatal_count,
        count(case when upper(a1.commercial_license) = 'YES' then 1 end) commercial_license_count,
        count(case when upper(a1.hazmat) = 'YES' then 1 end) hazmat_count,
        count(case when upper(a1.commercial_vehicle) = 'YES' then 1 end) commercial_vehicle_count,
        count(case when upper(a1.alcohol) = 'YES' then 1 end) alcohol_count,
        count(case when upper(a1.work_zone) = 'YES' then 1 end) work_zone_count,
        count(case when upper(a4.gender) = 'M' then 1 end) male_count,
        count(case when upper(a4.gender) = 'F' then 1 end) female_count,
        count(case when upper(a4.race) = 'ASIAN' then 1 end) asian_race_count,
        count(case when upper(a4.race) = 'BLACK' then 1 end) black_race_count,
        count(case when upper(a4.race) = 'hispanic' then 1 end) hispanic_race_count,
        count(case when upper(a4.race) = 'NATIVE AMERICAN' then 1 end) native_american_race_count,
        count(case when upper(a4.race) = 'WHITE' then 1 end) white_race_count,
        count(case when upper(a4.race) = 'OTHER' then 1 end) other_race_count
        from fact_traffic_violations a1
        JOIN dim_date a2 on a1.id_date = a2.id_date
        JOIN dim_subagency a3 ON a1.subagency_id = a3.subagency_id
		JOIN dim_driver a4 ON a1.driver_id = a4.driver_id
        group by a2.id_date, a3.subagency_id
    """

    #Load data into dim_agency table
    load_dim_agency = """
agency_lkup = df[['agency']].drop_duplicates().reset_index(drop=True)
agency_lkup['agency_id'] = agency_lkup.index+1
agency_lkup[['agency_id','agency']].to_sql(self.table,self.engine,if_exists="append",chunksize=5000,index=False,method='multi')
    """
    #Load data into dim_acrs table
    load_dim_acrs = """
df_acrs = df[['acrs_report_type']].drop_duplicates().reset_index(drop=True)
df_acrs['acrs_id'] = df_acrs.index+1
df_acrs[['acrs_id','acrs_report_type']].to_sql(self.table,self.engine,if_exists="append",chunksize=5000,index=False,method='multi')
    """
    #Load data into create_dim_route_type table
    load_dim_route_type = """
df_route_type = df[['route_type']].drop_duplicates().dropna()
df_route_type.reset_index(drop=True,inplace=True)
df_route_type['route_type_id'] = df_route_type.index+1
df_route_type[['route_type_id','route_type']].to_sql(self.table,self.engine,if_exists="append",chunksize=5000,index=False,method='multi')
    """
    #Load data into dim_municipality table
    load_dim_municipality = """
df_municipality = df[['municipality']].drop_duplicates().dropna().reset_index(drop=True)
df_municipality['municipality_id'] = df_municipality.index+1
df_municipality[['municipality_id','municipality']].to_sql(self.table,self.engine,if_exists="append",chunksize=5000,index=False,method='multi')
    """
    #Load data into dim_crashed_vehicle
    load_dim_crashed_vehicle = """
dfed_vehicle = df[['vehicle_id','vehicle_year','vehicle_make','vehicle_model']].drop_duplicates().dropna()
dfed_vehicle.to_sql(self.table,self.engine,if_exists="append",chunksize=5000,index=False,method='multi')
    """
    #Load data into dim_cross_street_type table
    load_dim_cross_street_type = """
df_cross_street = df[['cross_street_type','cross_street_name']].dropna().drop_duplicates().sort_values(by=['cross_street_type','cross_street_name']).reset_index(drop=True)
df_cross_street['cross_street_id'] = df_cross_street.index+1
df_cross_street[['cross_street_id','cross_street_type','cross_street_name']].to_sql(self.table,self.engine,if_exists="append",chunksize=5000,index=False,method='multi')
    """
    #Load data into fact_crash_details table
    load_fact_crash_details = """
df_agency = df[['agency']].drop_duplicates().reset_index(drop=True)
df_agency['agency_id'] = df_agency.index+1

df_acrs = df[['acrs_report_type']].drop_duplicates().reset_index(drop=True)
df_acrs['acrs_id'] = df_acrs.index+1

df_route_type = df[['route_type']].drop_duplicates().dropna()
df_route_type.reset_index(drop=True,inplace=True)
df_route_type['route_type_id'] = df_route_type.index+1

df_municipality = df[['municipality']].drop_duplicates().dropna().reset_index(drop=True)
df_municipality['municipality_id'] = df_municipality.index+1

dfed_vehicle = df[['vehicle_id','vehicle_year','vehicle_make','vehicle_model']].drop_duplicates().dropna()

df_cross_street = df[['cross_street_type','cross_street_name']].dropna().drop_duplicates().sort_values(by=['cross_street_type','cross_street_name']).reset_index(drop=True)
df_cross_street['cross_street_id'] = df_cross_street.index+1

self.cur.execute("select id_date,date_actual from dim_date")
df_date = pd.DataFrame(self.cur.fetchall(),columns=['id_date','date_actual'])
df_date['date_actual'] = pd.to_datetime(df_date.date_actual)

df = pd.merge(df,df_agency,left_on="agency",right_on="agency",how="left")
df = pd.merge(df,df_acrs,left_on="acrs_report_type",right_on="acrs_report_type",how="left")
df = pd.merge(df,df_route_type,left_on="route_type",right_on="route_type",how='left')
df = pd.merge(df,df_municipality,left_on="municipality",right_on="municipality",how='left')
df = pd.merge(df,df_cross_street,left_on=['cross_street_type','cross_street_name'],right_on=['cross_street_type','cross_street_name'],how='left')
df['crash_datetime'] = pd.to_datetime((df.crash_datetime.str.slice(start=0,stop=10)))
df = pd.merge(df,df_date,left_on='crash_datetime',right_on='date_actual',how='left')

df = df.reset_index(drop=True)
df['record_id']=df.index+1

self.cur.execute("Select * FROM fact_crash_details limit 0")
colnames = [desc[0] for desc in self.cur.description]
df = df[colnames]

df.to_csv("/home/arnabguha/Projects/Datasets/fact/fact_crash_details.csv", encoding='utf-8', index=False)

code = "COPY fact_crash_details FROM STDIN DELIMITER ',' CSV HEADER"
self.cur.copy_expert(code, open("/home/arnabguha/Projects/Datasets/fact/fact_crash_details.csv", "r"))
    """
