class sql:
    """This class has variables containing te SQL codes necessary for creatin the fact and dimension tables. It also contains the SQL codes for data quality checks. """
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
    #create dim_vehicle_type table
    create_dim_vehicle_type = """
        CREATE TABLE IF NOT EXISTS dim_vehicle_type (
            vehicletype_id INTEGER PRIMARY KEY,
            vehicletype TEXT
        )
    """
    #create dim_driver table
    create_dim_driver = """
        CREATE TABLE IF NOT EXISTS dim_driver (
            driver_id INTEGER PRIMARY KEY,
            gender TEXT,
            race TEXT,
            driver_city TEXT,
            driver_state TEXT,
            driver_dl_state TEXT
        )
    """
    #create dim_arrest_type table
    create_dim_arrest_type = """
        CREATE TABLE IF NOT EXISTS dim_arrest_type (
            arrest_type_id INTEGER PRIMARY KEY,
            arrest_type TEXT
        )
    """
    #create dim_violation_type table
    create_dim_violation_type = """
        CREATE TABLE IF NOT EXISTS dim_violation_type (
            violation_type_id INTEGER PRIMARY KEY,
            violation_type TEXT
        )
    """
    #create dim_subagency table
    create_dim_subagency = """
        CREATE TABLE IF NOT EXISTS dim_subagency (
            subagency_id INTEGER PRIMARY KEY,
            subagency TEXT,
            agency TEXT
        )
    """
    #create_fact_traffic_violations table
    create_fact_traffic_violations = """
        CREATE TABLE fact_traffic_violations (
           record_id INTEGER PRIMARY KEY,
           id_date INTEGER REFERENCES dim_date,
           time_of_stop TEXT,
           subagency_id INTEGER REFERENCES dim_subagency,
           description TEXT,
           location TEXT,
           latitude NUMERIC,
           longitude NUMERIC,
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
           vehicletype_id INTEGER REFERENCES dim_vehicle_type,
           year INTEGER,
           make TEXT,
           model TEXT,
           color TEXT,
           violation_type_id INTEGER REFERENCES dim_violation_type,
           charge TEXT,
           article TEXT,
           accident_contribution TEXT,
           driver_id INTEGER REFERENCES dim_driver,
           arrest_type_id INTEGER REFERENCES dim_arrest_type,
           geolocation TEXT
        );
    """

    #create_fact_traffic_violations_count_agg
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

    #Create dim_agency table
    create_dim_agency = """
        CREATE TABLE dim_agency (
            agency_id INTEGER PRIMARY KEY,
            agency TEXT
        )
    """

    #create dim_acrs_type
    create_dim_acrs_type = """
        CREATE TABLE dim_acrs_type (
            acrs_id INTEGER PRIMARY KEY,
            acrs_report_type TEXT
        )
    """
    #create dim_route_type
    create_dim_route_type = """
        CREATE TABLE dim_route_type (
            route_type_id INTEGER PRIMARY KEY,
            route_type TEXT
        )
    """

    #create dim_municipality
    create_dim_municipality = """
        CREATE TABLE dim_municipality (
            municipality_id INTEGER PRIMARY KEY,
            municipality TEXT
        )
    """

    #create dim_crashed_vehicle
    create_dim_crashed_vehicle = """
        CREATE TABLE dim_crashed_vehicle (
            vehicle_id TEXT PRIMARY KEY,
            vehicle_year INTEGER,
            vehicle_make TEXT,
            vehicle_model TEXT
        )
    """
    #create dim_cross_street_type table
    create_dim_cross_street_type = """
        CREATE TABLE dim_cross_street_type (
            cross_street_id INTEGER PRIMARY KEY,
            cross_street_type TEXT,
            cross_street_name TEXT
        )
    """
    #create fact_crash_details table
    create_fact_crash_details = """
    CREATE TABLE fact_crash_details (
        record_id INTEGER PRIMARY KEY,
        report_number TEXT,
        local_case_number TEXT,
        agency_id INTEGER,
        acrs_id INTEGER,
        id_date INTEGER,
        route_type_id TEXT,
        road_name TEXT,
        cross_street_id INTEGER,
        off_road_desc TEXT,
        municipality_id INTEGER,
        related_non_motorist TEXT,
        collision_type TEXT,
        weather TEXT,
        surface_condition TEXT,
        light TEXT,
        traffic_control TEXT,
        driver_substance_abuse TEXT,
        non_motorist_substance_abuse TEXT,
        person_id TEXT,
        driver_at_fault TEXT,
        injury_severity TEXT,
        circumstances TEXT,
        driver_distracted_by TEXT,
        driver_license_state TEXT,
        vehicle_id TEXT,
        vehicle_damage_extent TEXT,
        vehicle_first_impact_location TEXT,
        vehicle_second_impact_location TEXT,
        vehicle_body_type TEXT,
        vehicle_movement TEXT,
        vehicle_continuing_dir TEXT,
        vehicle_going_dir TEXT,
        speed_limit INTEGER,
        driverless_vehicle TEXT,
        parked_vehicle TEXT,
        equipment_problems TEXT,
        latitude NUMERIC,
        longitude NUMERIC
    )
    """
    #Data quality check SQL
    fact_traffic_violations_null_check = """
    select count(*) from fact_traffic_violations where (fact_traffic_violations is null)
    """
    fact_traffic_violations_has_rows = """
    select count(*) from fact_traffic_violations
    """
    fact_traffic_violations_count_agg_null_check = """
    select count(*) from fact_traffic_violations_count_agg where (fact_traffic_violations_count_agg is null)
    """
    fact_traffic_violations_count_agg_has_rows = """
    select count(*) from fact_traffic_violations_count_agg
    """
    fact_crash_details_null_check = """
    select count(*) from fact_crash_details where (fact_crash_details is null)
    """
    fact_crash_details_has_rows = """
    select count(*) from fact_crash_details
    """
