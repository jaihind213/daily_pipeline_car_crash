crashes_ddl = """
    CREATE TABLE %s.%s.crashes (
      workers_present_i             boolean,
      work_zone_type                string,
      work_zone_i                   boolean,
      weather_condition             string,
      trafficway_type               string,
      traffic_control_device        string,
      street_no                     int,
      street_name                   string,
      street_direction              string,
      statements_taken_i            boolean,
      sec_contributory_cause        string,
      roadway_surface_cond          string,
      road_defect                   string,
      prim_contributory_cause       string,
      posted_speed_limit            int,
      photos_taken_i                boolean,
      num_units                     int,
      not_right_of_way_i            boolean,
      most_severe_injury            string,
      longitude                     double,
      location                      string,
      lighting_condition            string,
      latitude                      double,
      lane_cnt                      int,
      intersection_related_i        boolean,
      injuries_unknown              int,
      injuries_total                int,
      injuries_reported_not_evident int,
      injuries_non_incapacitating   int,
      injuries_no_indication        int,
      injuries_incapacitating       int,
      injuries_fatal                int,
      hit_and_run_i                 boolean,
      dooring_i                     boolean,
      device_condition              string,
      damage                        string,
      damage_estimate               int,
      crash_type                    string,
      crash_record_id               string,
      crash_month                   int,
      crash_hour                    int,
      crash_day_of_week             int,
      crash_date_est_i              boolean,
      crash_date                    timestamp,
      date_police_notified          timestamp,
      beat_of_occurrence            int,
      alignment                     string,
      report_type                   string,
      first_crash_type              string,
      is_am boolean -- # noqa: W291    
    )
    USING iceberg
    PARTITIONED BY (
        days(crash_date)     -- Partition by day
    );
"""

crashes_people_ddl = """
CREATE TABLE %s.%s.crashes_people (
    person_id              string,
    person_type            string,
    crash_record_id        string,
    vehicle_id             string,
    crash_date             timestamp,
    seat_no                int,
    city                   string,
    state                  string,
    zipcode                string,
    sex                    string,
    age                    int,
    drivers_license_state  string,
    drivers_license_class  string,
    safety_equipment       string,
    airbag_deployed        boolean,
    airbag_deployed_txt    string,
    ejection               string,
    injury_classification  string,
    hospital               string,
    ems_agency             string,
    ems_run_no             string,
    driver_action          string,
    driver_vision          string,
    physical_condition     string,
    pedpedal_action        string,
    pedpedal_visibility    string,
    pedpedal_location      string,
    bac_result             string,
    bac_result_value       double,
    cell_phone_use         boolean,
    cell_phone_use_txt     string
)
USING iceberg
    PARTITIONED BY (
        days(crash_date)     -- Partition by day
    );
"""

crashes_vehicles_ddl = """
CREATE TABLE %s.%s.crashes_vehicles (
    crash_unit_id BIGINT,
    crash_record_id STRING,
    crash_date TIMESTAMP,
    unit_no BIGINT,
    unit_type STRING,
    num_passengers BIGINT,
    vehicle_id BIGINT,
    cmrc_veh_i boolean,
    make STRING,
    model STRING,
    lic_plate_state STRING,
    vehicle_year STRING,
    vehicle_defect STRING,
    veh_type_i boolean,
    vehicle_use STRING,
    travel_direction STRING,
    maneuver STRING,
    towed_i boolean,
    fire_i boolean,
    occupant_cnt BIGINT,
    exceed_speed_limit_i boolean,
    towed_by STRING,
    towed_to STRING,
    area_00_i boolean,
    area_01_i boolean,
    area_02_i boolean,
    area_03_i boolean,
    area_04_i boolean,
    area_05_i boolean,
    area_06_i boolean,
    area_07_i boolean,
    area_08_i boolean,
    area_09_i boolean,
    area_10_i boolean,
    area_11_i boolean,
    area_12_i boolean,
    area_99_i boolean   ,
    first_contact_point STRING,
    cmv_id BIGINT,
    usdot_no STRING,
    ccmc_no STRING,
    ilcc_no STRING,
    commercial_src STRING,
    gvwr STRING,
    carrier_name STRING,
    carrier_state STRING,
    carrier_city STRING,
    hazmat_placards_i boolean,
    hazmat_name STRING,
    un_no STRING,
    hazmat_present_i boolean,
    hazmat_report_i boolean,
    hazmat_report_no boolean,
    hazmat_viol_cause_crash_i boolean,
    mcs_viol_cause_crash_i boolean,
    idot_permit_no STRING,
    wide_load_i boolean,
    trailer1_width STRING,
    trailer2_width STRING,
    trailer1_length BIGINT,
    trailer2_length BIGINT,
    total_vehicle_length STRING,
    axle_cnt BIGINT,
    vehicle_config STRING,
    cargo_body_type STRING,
    load_type STRING,
    hazmat_out_of_service_i boolean,
    mcs_out_of_service_i boolean,
    hazmat_class STRING
)
USING iceberg
    PARTITIONED BY (
        days(crash_date)     -- Partition by day
    );
"""

test_ddl = """
CREATE TABLE %s.%s.one (
    id              string,
    crash_date      timestamp
)
USING iceberg
    PARTITIONED BY (
        days(crash_date)     -- Partition by day
    );
"""

test1_ddl = """
CREATE TABLE %s.%s.two (
    id              string,
    crash_date      timestamp
)
USING iceberg
    PARTITIONED BY (
        days(crash_date)     -- Partition by day
    );
"""

crashes_ppl_summary_ddl = """
CREATE TABLE %s.%s.crashes_people_summary (
    crash_date timestamp,
    crash_record_id STRING,
    is_am boolean,
    damage int,
    number_injured INT,
    number_fatal INT,
    weather_condition STRING,
    lighting_condition STRING,
    is_hit_run BOOLEAN,
    street_name STRING,
    lat DOUBLE,
    lon DOUBLE,
    road_defect STRING,
    is_cell_phone_use BOOLEAN,
    is_drunk BOOLEAN,
    list_of_hospital ARRAY<STRING>,
    list_of_gender ARRAY<STRING>,
    is_age_below_5 BOOLEAN,
    is_age_10s BOOLEAN,
    is_age_20s BOOLEAN,
    is_age_30s BOOLEAN,
    is_age_40s BOOLEAN,
    is_age_50s BOOLEAN,
    is_age_senior BOOLEAN,
    list_of_airbag_deployed ARRAY<STRING>
)
USING iceberg
PARTITIONED BY (days(crash_date));
"""

crashes_ppl_vehicle_summary_ddl = """
CREATE TABLE %s.%s.crashes_people_vehicle_summary (
    crash_date timestamp,
    crash_record_id STRING,
    is_am boolean,
    damage int,
    number_injured INT,
    number_fatal INT,
    weather_condition STRING,
    lighting_condition STRING,
    is_hit_run BOOLEAN,
    street_name STRING,
    lat DOUBLE,
    lon DOUBLE,
    road_defect STRING,
    is_cell_phone_use BOOLEAN,
    is_drunk BOOLEAN,
    list_of_hospital ARRAY<STRING>,
    list_of_gender ARRAY<STRING>,
    is_age_below_5 BOOLEAN,
    is_age_10s BOOLEAN,
    is_age_20s BOOLEAN,
    is_age_30s BOOLEAN,
    is_age_40s BOOLEAN,
    is_age_50s BOOLEAN,
    is_age_senior BOOLEAN,
    list_of_airbag_deployed ARRAY<STRING>,
    vehicle_uses ARRAY<STRING>,
    vehicle_makes ARRAY<STRING>,
    fire_happened BOOLEAN,
    hazmat_involved BOOLEAN,
    num_people_involved INT
)
USING iceberg
PARTITIONED BY (days(crash_date));
"""

crashes_cube_ddl = """
CREATE TABLE %s.%s.crashes_cube (
    crash_day date,
    is_am boolean,
    total_damage int,
    total_injured INT,
    total_fatal INT,
    weather_condition STRING,
    lighting_condition STRING,
    is_hit_run BOOLEAN,
    street_name STRING,
    lat DOUBLE,
    lon DOUBLE,
    road_defect STRING,
    is_cell_phone_use BOOLEAN,
    is_drunk BOOLEAN,
    list_of_hospital ARRAY<STRING>,
    list_of_gender ARRAY<STRING>,
    is_age_below_5 BOOLEAN,
    is_age_10s BOOLEAN,
    is_age_20s BOOLEAN,
    is_age_30s BOOLEAN,
    is_age_40s BOOLEAN,
    is_age_50s BOOLEAN,
    is_age_senior BOOLEAN,
    list_of_airbag_deployed ARRAY<STRING>,
    vehicle_uses ARRAY<STRING>,
    vehicle_makes ARRAY<STRING>,
    fire_happened BOOLEAN,
    hazmat_involved BOOLEAN,
    total_people_involved INT,
    crashes_set_sketch BINARY
)
USING iceberg
PARTITIONED BY (days(crash_day));
"""

tables_ddl = {
    "crashes": crashes_ddl,
    "crashes_people": crashes_people_ddl,
    "crashes_vehicles": crashes_vehicles_ddl,
    "crashes_people_summary": crashes_ppl_summary_ddl,
    "crashes_people_vehicle_summary": crashes_ppl_vehicle_summary_ddl,
    "crashes_cube": crashes_cube_ddl,
}
