import pandas as pd
from grptavutils import (
    list_blob_files, read_parquet, read_csv, write_parquet,
    delete_blob_file, delete_staging_files, read_excel

)
from grptavutils.constants import Fields, Storage
from grptavutils.logs import logger


def read_employee_hours():

    df = read_excel(Storage.bronze, Storage.employee_hours)
    df = df.drop(columns=[Fields.ora_employee_first_name, Fields.ora_employee_last_name])

    return df

def read_bronze():
    try:
        df = read_parquet(Storage.bronze, Storage.bronze_oracle_guests)
        return df

    except FileNotFoundError:
        empty_df = pd.DataFrame(columns=[
            Fields.filename,
            Fields.ora_guest_check_id,
            Fields.ora_employee_id,
            Fields.ora_rev_center_id,
            Fields.ora_location_id,
            Fields.ora_order_type_id,
            Fields.date,
            Fields.ora_check_open_datetime,
            Fields.ora_check_close_datetime,
            Fields.ora_num_guests,
            Fields.ora_check_total,
            Fields.ora_void_total,
            Fields.ora_tip_total,
            Fields.ora_error_correct_total,
            Fields.ora_transfer_status_code,
            Fields.ora_transfer_status,
            Fields.ora_transfer_to_check_num,
            Fields.ora_service_charge,
            Fields.ora_discount_total,
            Fields.ora_check_sub_total,
            Fields.ora_order_type_name,
            Fields.ora_guest_employee_first_name,
            Fields.ora_guest_employee_last_name,
            Fields.ora_check_duration,
            Fields.ora_table_reference,
            Fields.ora_check_tot_items,
            Fields.ora_error_correct_count,
            Fields.ora_is_employee_meal,
            Fields.hours_per_week,
        ])

        return empty_df


def read_staging():
    files = list_blob_files(container_name=Storage.staging, blob_path="oracle/Guest")

    # iterate over files
    df = pd.DataFrame()
    for f in files:
        try:
            current_df = read_csv(container=Storage.staging, file_path=f, sep=";", decimal=".")
            current_df.loc[:, Fields.filename] = f
            df = pd.concat([df, current_df])
        except pd.errors.EmptyDataError:
            delete_blob_file(container_name=Storage.staging, file_path=f)

    # rename columns
    ren_cols = {
        Fields.filename: Fields.filename,
        "openBusinessDate": Fields.date,
        "closeDateTime": Fields.ora_check_close_datetime,
        "openDateTime": Fields.ora_check_open_datetime,
        "guestCheckID": Fields.ora_guest_check_id,
        "employeeID": Fields.ora_employee_id,
        "revenueCenterID": Fields.ora_rev_center_id,
        "locationID": Fields.ora_location_id,
        "orderTypeID": Fields.ora_order_type_id,
        "numGuests": Fields.ora_num_guests,
        "checkTotal": Fields.ora_check_total,
        "voidTotal": Fields.ora_void_total,
        "tipTotal": Fields.ora_tip_total,
        "errorCorrectTotal": Fields.ora_error_correct_total,
        "transferToCheckNum": Fields.ora_transfer_to_check_num,
        "serviceChargeTotal": Fields.ora_service_charge,
        "discountTotal": Fields.ora_discount_total,
        "subTotal": Fields.ora_check_sub_total,
        "checkDuration": Fields.ora_check_duration,
        "numItems": Fields.ora_check_tot_items,
        "errorCorrectCount": Fields.ora_error_correct_count,
        "isEmployeeMealFlag": Fields.ora_is_employee_meal,
        "transferStatusCode": Fields.ora_transfer_status_code,
        "transferStatus": Fields.ora_transfer_status,
        "orderTypeName": Fields.ora_order_type_name,
        "employeeID": Fields.ora_guest_employee_id,
        "employeeFirstName": Fields.ora_guest_employee_first_name,
        "employeeLastName": Fields.ora_guest_employee_last_name,
        "tableReference": Fields.ora_table_reference,
    }
    df = df.rename(columns=ren_cols)

    # select
    sel = list(ren_cols.values())
    df = df[sel]

    # data types
    df[Fields.date] = pd.to_datetime(df[Fields.date], format="%Y-%m-%d")
    df[Fields.ora_check_close_datetime] = pd.to_datetime(df[Fields.ora_check_close_datetime], format="%Y-%m-%dT%H:%M:%S")
    df[Fields.ora_check_open_datetime] = pd.to_datetime(df[Fields.ora_check_open_datetime], format="%Y-%m-%dT%H:%M:%S")
    df[Fields.ora_guest_check_id] = df[Fields.ora_guest_check_id].astype("int", errors="ignore")
    df[Fields.ora_guest_employee_id] = df[Fields.ora_guest_employee_id].astype("int", errors="ignore")
    df[Fields.ora_rev_center_id] = df[Fields.ora_rev_center_id].astype("int", errors="ignore")
    df[Fields.ora_location_id] = df[Fields.ora_location_id].astype("int", errors="ignore")
    df[Fields.ora_order_type_id] = df[Fields.ora_order_type_id].astype("int", errors="ignore")
    df[Fields.ora_num_guests] = df[Fields.ora_num_guests].astype("int", errors="ignore")
    df[Fields.ora_check_total] = df[Fields.ora_check_total].astype("double", errors="ignore")
    df[Fields.ora_void_total] = df[Fields.ora_void_total].astype("double", errors="ignore")
    df[Fields.ora_tip_total] = df[Fields.ora_tip_total].astype("double", errors="ignore")
    df[Fields.ora_error_correct_total] = df[Fields.ora_error_correct_total].astype("double", errors="ignore")
    df[Fields.ora_transfer_to_check_num] = df[Fields.ora_transfer_to_check_num].astype("int", errors="ignore")
    df[Fields.ora_service_charge] = df[Fields.ora_service_charge].astype("double", errors="ignore")
    df[Fields.ora_discount_total] = df[Fields.ora_discount_total].astype("double", errors="ignore")
    df[Fields.ora_check_sub_total] = df[Fields.ora_check_sub_total].astype("double", errors="ignore")
    df[Fields.ora_check_duration] = df[Fields.ora_check_duration].astype("int", errors="ignore")
    df[Fields.ora_check_tot_items] = df[Fields.ora_check_tot_items].astype("int", errors="ignore")
    df[Fields.ora_error_correct_count] = df[Fields.ora_error_correct_count].astype("int", errors="ignore")
    df[Fields.ora_is_employee_meal] = df[Fields.ora_is_employee_meal].astype("int", errors="ignore")

    # initcap
    df[Fields.ora_transfer_status_code] = df[Fields.ora_transfer_status_code].str.capitalize()
    df[Fields.ora_transfer_status] = df[Fields.ora_transfer_status].str.capitalize()
    df[Fields.ora_guest_employee_first_name] = df[Fields.ora_guest_employee_first_name].str.capitalize()
    df[Fields.ora_guest_employee_last_name] = df[Fields.ora_guest_employee_last_name].str.capitalize()
    df[Fields.ora_table_reference] = df[Fields.ora_table_reference].str.capitalize()

    # fill missing
    df[Fields.ora_guest_check_id] = df[Fields.ora_guest_check_id].fillna(0)
    df[Fields.ora_employee_id] = df[Fields.ora_employee_id].fillna(0)
    df[Fields.ora_rev_center_id] = df[Fields.ora_rev_center_id].fillna(0)
    df[Fields.ora_location_id] = df[Fields.ora_location_id].fillna(0)
    df[Fields.ora_order_type_id] = df[Fields.ora_order_type_id].fillna(0)
    df[Fields.ora_num_guests] = df[Fields.ora_num_guests].fillna(0)
    df[Fields.ora_check_total] = df[Fields.ora_check_total].fillna(0)
    df[Fields.ora_void_total] = df[Fields.ora_void_total].fillna(0)
    df[Fields.ora_tip_total] = df[Fields.ora_tip_total].fillna(0)
    df[Fields.ora_error_correct_total] = df[Fields.ora_error_correct_total].fillna(0)
    df[Fields.ora_transfer_to_check_num] = df[Fields.ora_transfer_to_check_num].fillna(0)
    df[Fields.ora_service_charge] = df[Fields.ora_service_charge].fillna(0)
    df[Fields.ora_discount_total] = df[Fields.ora_discount_total].fillna(0)
    df[Fields.ora_check_sub_total] = df[Fields.ora_check_sub_total].fillna(0)
    df[Fields.ora_check_duration] = df[Fields.ora_check_duration].fillna(0)
    df[Fields.ora_check_tot_items] = df[Fields.ora_check_tot_items].fillna(0)
    df[Fields.ora_error_correct_count] = df[Fields.ora_error_correct_count].fillna(0)
    df[Fields.ora_is_employee_meal] = df[Fields.ora_is_employee_meal].fillna(0)
    df[Fields.ora_transfer_status_code] = df[Fields.ora_transfer_status_code].fillna(Fields.missing)
    df[Fields.ora_transfer_status] = df[Fields.ora_transfer_status].fillna(Fields.missing)
    df[Fields.ora_guest_employee_first_name] = df[Fields.ora_guest_employee_first_name].fillna(Fields.missing)
    df[Fields.ora_guest_employee_last_name] = df[Fields.ora_guest_employee_last_name].fillna(Fields.missing)
    df[Fields.ora_table_reference] = df[Fields.ora_table_reference].fillna(Fields.missing)

    return df


def trunc_staging(bronze_df, staging_df):
    dates = bronze_df[Fields.date].unique()
    mask = staging_df[Fields.date].isin(dates)
    trunc_df = staging_df[~mask]

    return trunc_df

def day_period(df_in: pd.DataFrame) -> None:

    df_in[Fields.period_of_day] = df_in["check_close_datetime"].dt.hour.map({
            0: "Cena",
            1: "Cena",
            2: "Cena",
            3: "Cena",
            4: "Cena",
            5: "Cena",
            6: "Cena",
            7: "Pranzo",
            8: "Pranzo",
            9: "Pranzo",
            10: "Pranzo",
            11: "Pranzo",
            12: "Pranzo",
            13: "Pranzo",
            14: "Pranzo",
            15: "Pranzo",
            16: "Pranzo",
            17: "Pranzo",
            18: "Cena",
            19: "Cena",
            20: "Cena",
            21: "Cena",
            22: "Cena",
            23: "Cena",
        })

    df_in[Fields.shift_id] = df_in[Fields.ora_employee_id].astype(str) + "//" + df_in[Fields.period_of_day]

    return df_in

def calc_productivity(df_in):

    # read productivity data and make key
    emp_df = read_employee_hours()
    emp_df[Fields.shift_id] = emp_df[Fields.ora_employee_id].astype(str) + "//" + emp_df[Fields.period_of_day]
    emp_df = emp_df[[Fields.shift_id, Fields.hours_per_week]]

    # force 0 hours to null
    zero_hours = emp_df[Fields.hours_per_week] == 0
    emp_df.loc[zero_hours, Fields.hours_per_week] = None

    # ensure no duplicates
    emp_df = emp_df.drop_duplicates(subset=[Fields.shift_id])

    # join and calculate productivity
    df = (
        df_in
        .drop(columns=Fields.hours_per_week)
        .merge(emp_df, on=Fields.shift_id, how="left")
    )
    df[Fields.productivity] = df[Fields.ora_check_total] / df[Fields.hours_per_week]

    # fill missing
    df[Fields.productivity] = df[Fields.productivity].fillna(0)
    df[Fields.hours_per_week] = df[Fields.hours_per_week].fillna(0)

    return df




def main():

    # read bronze
    bronze_df = read_bronze()

    # read staging
    staging_df = read_staging()

    # drop form staging already available data
    trunc_df = trunc_staging(bronze_df=bronze_df, staging_df=staging_df)

    # append to bronze
    df = pd.concat([
        bronze_df,
        trunc_df.drop(columns=Fields.filename)
    ])

    # make day period
    day_period(df)

    # calculate productivity
    df = calc_productivity(df)

    # drop duplicates
    df = df.drop_duplicates(subset=[Fields.ora_guest_check_id])

    # write
    write_parquet(
        dataframe=df,
        container=Storage.bronze,
        file_path=Storage.bronze_oracle_guests,
    )

    # delete files from blob
    files = list(staging_df[Fields.filename].unique())
    #delete_staging_files(files)


if __name__ == "__main__":
    main()
