class Fields:

    # datasets columns

    # generic
    filename = "filename"
    date = "date"
    execution_time = "execution_time"
    relative_days = "relative_days"
    relative_weeks = "relative_weeks"
    day_of_week = "day_of_week"

    # google analytics
    ga_source = "ga_source"
    ga_channel_grouping = "ga_channel_grouping"
    ga_sessions = "ga_sessions"
    ga_users = "ga_users"

    # oracle employees
    ora_location_id = "location_id"
    ora_rev_center_id = "revenue_center_id"
    ora_ora_rev_center_name = "revenue_center_name"
    ora_employee_id = "employee_id"
    ora_employee_first_name = "employee_first_name"
    ora_employee_last_name = "employee_last_name"
    ora_menu_item_id = "menu_item_id"
    ora_menu_item_name = "menu_item_name"
    ora_menu_item_master_name = "menu_item_master_name"
    ora_major_group_name = "major_group_name"
    ora_family_group_name = "family_group_name"
    ora_sales_total = "sales_total"
    ora_sales_count = "sales_count"
    ora_sales_gross_before_discount = "sales_gross_before_discount"
    ora_discount_total = "discount_total"

    # oracle guest
    ora_guest_check_id = "guest_check_id"
    ora_order_type_id = "order_type_id"
    ora_check_open_datetime = "check_open_datetime"
    ora_check_close_datetime = "check_close_datetime"
    ora_num_guests = "num_guests"
    ora_check_total = "check_total"
    ora_void_total = "void_total"
    ora_tip_total = "tip_total"
    ora_error_correct_total = "error_correct_total"
    ora_transfer_status_code = "transfer_status_code"
    ora_transfer_status = "transfer_status"
    ora_transfer_to_check_num = "transfer_to_check_num"
    ora_service_charge = "service_charge"
    ora_check_sub_total = "check_sub_total"
    ora_order_type_name = "order_type_name"
    ora_guest_employee_id = ora_employee_id
    ora_guest_employee_first_name = ora_employee_first_name
    ora_guest_employee_last_name = ora_employee_last_name
    ora_check_duration = "check_duration"
    ora_table_reference = "table_reference"
    ora_check_tot_items = "check_tot_items"
    ora_error_correct_count = "error_correct_count"
    ora_is_employee_meal = "is_employee_meal"

    missing = "_missing_"

    relative_cycle_28 = "relative_cycle_28"
    relative_cycle_7 = "relative_cycle_7"

    fcst_observation = "data_point_class"

    period_of_day = "period_of_day"
    shift_id = "shift_id"

class Storage:

    # container names
    staging = "staging"
    bronze = "bronze"
    silver = "silver"
    account_name = "gruppotavolastorage"

    # filepaths
    bronze_ga = "googleanalytics/googleanalytics.parquet"
    bronze_oracle_employees = "oracle/employees.parquet"
    bronze_oracle_guests = "oracle/guests.parquet"
    silver_dates = "dates/dates.parquet"
    silver_employees = "employees/employees.parquet"
    silver_items = "items/items.parquet"
    bronze_forecast = "forecast/forecast.parquet"
