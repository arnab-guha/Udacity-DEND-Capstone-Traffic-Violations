class capstone_sql_queries:
    #create stage_traffic table
    create_stg_traffic_violaitions = """
        CREATE TABLE IF NOT EXISTS stg_traffic_violations (
        date_of_stop TEXT,
        time_of_stop TEXT,
        agency TEXT,
        subagency TEXT,
        description TEXT,
        location TEXT,
        latitude TEXT,
        longitude TEXT,
        accident TEXT,
        belts TEXT,
        personal_injury TEXT,
        property_damage TEXT,
        fatal TEXT,
        commercial_license TEXT,
        hazmat TEXT,
        commercial_vehicle TEXT,
        alcohol TEXT,
        work_zone TEXT,
        state TEXT,
        vehicletype TEXT,
        year TEXT,
        make TEXT,
        model TEXT,
        color TEXT,
        violation_type TEXT,
        charge TEXT,
        article TEXT,
        contributed_to_accident TEXT,
        race TEXT,
        gender TEXT,
        driver_city TEXT,
        driver_state TEXT,
        dL_state TEXT,
        arrest_type TEXT,
        geolocation TEXT
    )
    """
    #create dim_state table
    create_dim_state = """
        CREATE TABLE IF NOT EXISTS dim_state (
            state_code TEXT,
            country_name TEXT,
            state_desc TEXT,
            PRIMARY KEY(state_code,country_name)
        )
    """
    #create dim_race table
    create_dim_race = """
        CREATE TABLE IF NOT EXISTS dim_race(
            race_id INTEGER PRIMARY KEY,
            race TEXT
        )
    """
    #insert dim_race table
    insert_dim_race = """
        INSERT INTO dim_race
        SELECT DISTINCT
        DENSE_RANK() OVER(ORDER BY race) race_id,
        race
        FROM stg_traffic_violations
    """
    #create dim_driver table
    create_dim_driver = """
        CREATE TABLE IF NOT EXISTS dim_driver (
            driver_id INTEGER,
            gender TEXT,
            race_id INTEGER,
            driver_city TEXT,
            driver_state TEXT,
            driver_dl_state TEXT,
            driver_country TEXT
        )
    """
    #insert dim_driver table
    insert_dim_driver = """
        INSERT INTO dim_driver
        SELECT DISTINCT
        DENSE_RANK() OVER(ORDER BY a1.gender,a2,race_id,a1.driver_city,a1.driver_state, a1.dl_state,a3.country_name) driver_id,
        a1.gender,
        a2.race_id,
        a1.driver_city,
        a1.driver_state,
        a1.dl_state,
        a3.country_name
        FROM stg_traffic_violations a1
        JOIN dim_race a2 ON a1.race = a2.race
        JOIN dim_state a3 ON a1.dl_state = a3.state_code
    """

    #create dim_date table
    create_dim_date = """
        CREATE TABLE IF NOT EXISTS dim_date (
        id_date  INTEGER PRIMARY KEY,
        date_actual DATE,
        day_name TEXT,
        day_of_week INTEGER,
        day_of_month INTEGER,
        current_date_flag INTEGER,
        previous_date_flag INTEGER,
        next_date_flag INTEGER,
        week_of_month INTEGER,
        week_of_year INTEGER,
        month_number INTEGER,
        month_name TEXT,
        month_name_abbr TEXT,
        quarter_actual INTEGER,
        quarter_name TEXT,
        year_actual INTEGER,
        week_start_date DATE,
        week_end_date DATE,
        monh_start_date DATE,
        month_end_date DATE,
        quarter_start_date DATE,
        quarter_end_date DATE,
        year_start_date DATE,
        year_end_date DATE,
        weekend_ind TEXT
        )
    """
    #insert dim_date table
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
    #create dim_vehicle_type table
    create_dim_vehicletype = """
        CREATE TABLE IF NOT EXISTS dim_vehicletype (
            vehicletype_id INTEGER PRIMARY KEY,
            vehicletype_desc TEXT,
            year_of_mfg INTEGER,
            make TEXT,
            model TEXT,
            color TEXT
        )
    """
    #insert dim_vehicle_type table
    insert_dim_vehicletype = """
        INSERT INTO dim_vehicletype
        SELECT DISTINCT
        DENSE_RANK() OVER (ORDER BY  vehicletype,year,make,model,color) AS vehicletype_id,
        vehicletype,
        CAST(CAST(year AS FLOAT) AS INTEGER) ,
        make,
        model,
        color
        FROM stg_traffic_violations
    """
    #create dim_arrest_type table
    create_dim_arrest_type = """
        CREATE TABLE IF NOT EXISTS dim_arrest_type (
            arrest_type_id CHAR PRIMARY KEY,
            arrest_type_desc TEXT
        )
    """
    #insert dim_arrest_type table
    insert_dim_arrest_type = """
        INSERT INTO dim_arrest_type
        SELECT DISTINCT
        SUBSTRING(arrest_type from 1 for 1),
        SUBSTRING(arrest_type from 5 for CHAR_LENGTH(arrest_type)-3)
        FROM stg_traffic_violations
    """
    #create dim_subagency table
    create_dim_subagency = """
        CREATE TABLE IF NOT EXISTS dim_subagency (
            subagency_id INTEGER PRIMARY KEY,
            subagency_desc TEXT,
            agency_desc TEXT
        )
    """
    #insert dim_subagency table
    insert_dim_subagency = """
        INSERT INTO dim_subagency
        SELECT distinct
        DENSE_RANK() OVER (ORDER BY subagency) AS subagency_id,
        subagency,
        agency
        FROM stg_traffic_violations
    """
    #create dim_violation_type table
    create_dim_violation_type = """
        CREATE TABLE IF NOT EXISTS dim_violation_type (
            violation_type_id INTEGER PRIMARY KEY,
            violation_type TEXT
        )
    """
    #insert dim_violation_type table
    insert_dim_violation_type = """
        INSERT INTO dim_violation_type
        SELECT DISTINCT
        DENSE_RANK() OVER(ORDER BY violation_type) violation_type_id,
        violation_type
        FROM stg_traffic_violations
    """
    #create fact_traffic_violations table
    create_fact_traffic_violations = """
        CREATE TABLE fact_traffic_violations (
           id_date INTEGER,
           time_of_stop TEXT,
           subagency_id INTEGER,
           stop_desc TEXT,
           location TEXT,
           latitude NUMERIC,
           longitude NUMERIC,
           flag_accident TEXT,
           flag_belts TEXT,
           flag_personal_injury TEXT,
           flag_property_damage TEXT,
           flag_fatal TEXT,
           flag_commercial_license TEXT,
           flag_hazmat TEXT,
           flag_commercial_vehicle TEXT,
           flag_alcohol TEXT,
           flag_work_zone TEXT,
           flag_state TEXT,
           vehicletype_id INTEGER,
           violation_type_id INTEGER,
           CHARge TEXT,
           article TEXT,
           accident_contribution TEXT,
           driver_id INTEGER,
           arrest_type_id CHAR(1),
           geolocation TEXT,
           PRIMARY KEY (id_date,time_of_stop,subagency_id,vehicletype_id,driver_id,arrest_type_id)
        );
    """
    #insert fact_traffic_violations table
    insert_fact_traffic_violations = """
        INSERT INTO fact_traffic_violations
        SELECT DISTINCT
        a2.id_date REFERENCES dim_date,
        a1.time_of_stop,
        a3.subagency_id REFERENCES dim_subagency,
        a1.description stop_desc,
        a1.location,
        a1.latitude,
        a1.longitude,
        a1.accident flag_accident,
        a1.belts flag_belts,
        a1.personal_injury flag_personal_injury,
        a1.property_damage flag_property_damage,
        a1.fatal flag_fatal,
        a1.commercial_license flag_commercial_license,
        a1.hazmat flag_hazmat,
        a1.commercial_vehicle flag_commercial_vehicle,
        a1.alcohol flag_alcohol,
        a1.work_zone flag_work_zone,
        a1.state flag_state,
        a4.vehicletype_id REFERENCES dim_vehicletype,
        a5.violation_type_id REFERENCES dim_violation_type,
        a1.charge,
        a1.article,
        a1.contributed_to_accident accident_contribution,
        a7.driver_id REFERENCES dim_driver,
        a8.arrest_type_id REFERENCES dim_arrest_type,
        a1.geolocation
        FROM
        stg_traffic_violations a1
        JOIN dim_date a2 ON CAST(a1.date_of_stop AS DATE) = a2.date_actual
        JOIN dim_subagency a3 ON a1.subagency = a3.subagency_desc
        JOIN dim_vehicletype a4 ON  (a1.vehicletype = a4.vehicletype_desc and
                                     CAST(CAST(a1.year AS FLOAT) AS INTEGER) = a4.year_of_mfg and
                                     a1.make = a4.make and
                                     a1.model = a4.model and
                                     a1.color = a4.color)
        JOIN dim_violation_type a5 ON a1.violation_type  = a5.violation_type
        JOIN dim_race a6 ON a1.race = a6.race
        JOIN dim_driver a7 ON   (a1.gender = a7.gender and
                                 a1.driver_city = a7.driver_city and
                                 a1.driver_state = a7.driver_state and
                                 a1.dl_state = a7.driver_dl_state)
        JOIN dim_arrest_type a8 ON SUBSTRING(a1.arrest_type from 5 for CHAR_LENGTH(a1.arrest_type)-3) = a8.arrest_type_desc
    """

    #create table fact_traffic_violations_count_agg
    create_fact_traffic_violations_count_agg = """
        CREATE TABLE fact_traffic_violations_count_agg (
            id_date INTEGER REFERENCES dim_date,
            subagency_id INTEGER REFERENCES dim_subagency,
            violation_count INTEGER,
            accidents_count INTEGER,
            belts_count INTEGER,
            personal_injury_count INTEGER,
            property_damage_count INTEGER,
            fatal_count INTEGER,
            commercial_license_count INTEGER,
            hazmat_count INTEGER,
            commercial_vehicle_count INTEGER,
            alcohol_count INTEGER,
            work_zone_count INTEGER,
            male_count INTEGER,
            female_count INTEGER,
            asian_race_count INTEGER,
            black_race_count INTEGER,
            hispanic_race_count INTEGER,
            native_american_count INTEGER,
            white_race_count INTEGER,
            other_race_count INTEGER,
            PRIMARY KEY (id_date,subagency_id)
         )
    """

    #insert table fact_traffic_violations_count_agg
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
        count(case when upper(a1.gender) = 'M' then 1 end) male_count,
        count(case when upper(a1.gender) = 'F' then 1 end) female_count,
        count(case when upper(a1.race) = 'ASIAN' then 1 end) asian_race_count,
        count(case when upper(a1.race) = 'BLACK' then 1 end) black_race_count,
        count(case when upper(a1.race) = 'hispanic' then 1 end) hispanic_race_count,
        count(case when upper(a1.race) = 'NATIVE AMERICAN' then 1 end) native_american_race_count,
        count(case when upper(a1.race) = 'WHITE' then 1 end) white_race_count,
        count(case when upper(a1.race) = 'OTHER' then 1 end) other_race_count
        from stg_traffic_violations a1
        JOIN dim_date a2 ON CAST(a1.date_of_stop AS DATE) = a2.date_actual
        JOIN dim_subagency a3 ON a1.subagency = a3.subagency_desc
        group by a2.id_date, a3.subagency_id
    """
