import pandas as pd
from grptavutils import list_blob_files, read_parquet, read_csv, write_parquet, delete_blob_file, delete_staging_files
from grptavutils.constants import Fields, Storage
from grptavutils.logs import logger

def read_bronze():
    try:
        df = read_parquet(Storage.bronze, Storage.bronze_oracle_employees)
        return df

    except FileNotFoundError:
        empty_df = pd.DataFrame(columns=[
            Fields.filename,
            Fields.date,
            Fields.ora_location_id,
            Fields.ora_rev_center_id,
            Fields.ora_employee_id,
            Fields.ora_menu_item_id,
            Fields.ora_ora_rev_center_name,
            Fields.ora_employee_first_name,
            Fields.ora_employee_last_name,
            Fields.ora_menu_item_name,
            Fields.ora_menu_item_master_name,
            Fields.ora_major_group_name,
            Fields.ora_family_group_name,
            Fields.ora_sales_total,
            Fields.ora_sales_count,
            Fields.ora_sales_gross_before_discount,
            Fields.ora_discount_total,
        ])

        return empty_df


def read_staging():
    files = list_blob_files(container_name=Storage.staging, blob_path="oracle/Employee")

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
        "businessDate": Fields.date,
        "locationID": Fields.ora_location_id,
        "revenueCenterID": Fields.ora_rev_center_id,
        "employeeID": Fields.ora_employee_id,
        "menuItemID": Fields.ora_menu_item_id,
        "revenueCenterName": Fields.ora_ora_rev_center_name,
        "employeeFirstName": Fields.ora_employee_first_name,
        "employeeLastName": Fields.ora_employee_last_name,
        "menuItemName": Fields.ora_menu_item_name,
        "menuItemMasterName": Fields.ora_menu_item_master_name,
        "majorGroupName": Fields.ora_major_group_name,
        "familyGroupName": Fields.ora_family_group_name,
        "salesTotal": Fields.ora_sales_total,
        "salesCount": Fields.ora_sales_count,
        "grossSalesBeforeDiscount": Fields.ora_sales_gross_before_discount,
        "discountTotal": Fields.ora_discount_total,
    }
    df = df.rename(columns=ren_cols)

    # select
    sel = list(ren_cols.values())
    df = df[sel]

    # data types
    df[Fields.date] = pd.to_datetime(df[Fields.date], format="%Y-%m-%d")
    df[Fields.ora_location_id] = df[Fields.ora_location_id].astype("int", errors="ignore")
    df[Fields.ora_rev_center_id] = df[Fields.ora_rev_center_id].astype("int", errors="ignore")
    df[Fields.ora_employee_id] = df[Fields.ora_employee_id].astype("int", errors="ignore")
    df[Fields.ora_menu_item_id] = df[Fields.ora_menu_item_id].astype("int", errors="ignore")
    df[Fields.ora_sales_total] = df[Fields.ora_sales_total].astype("double", errors="ignore")
    df[Fields.ora_sales_count] = df[Fields.ora_sales_count].astype("int", errors="ignore")
    df[Fields.ora_sales_gross_before_discount] = df[Fields.ora_sales_gross_before_discount].astype("double", errors="ignore")
    df[Fields.ora_discount_total] = df[Fields.ora_discount_total].astype("double", errors="ignore")

    # initcap
    df[Fields.ora_ora_rev_center_name] = df[Fields.ora_ora_rev_center_name].str.capitalize()
    df[Fields.ora_employee_first_name] = df[Fields.ora_employee_first_name].str.capitalize()
    df[Fields.ora_employee_last_name] = df[Fields.ora_employee_last_name].str.capitalize()
    df[Fields.ora_menu_item_name] = df[Fields.ora_menu_item_name].fillna(Fields.missing)
    df[Fields.ora_menu_item_master_name] = df[Fields.ora_menu_item_master_name].str.capitalize()
    df[Fields.ora_major_group_name] = df[Fields.ora_major_group_name].str.capitalize()
    df[Fields.ora_family_group_name] = df[Fields.ora_family_group_name].str.capitalize()


    # fill missing
    df[Fields.ora_ora_rev_center_name] = df[Fields.ora_ora_rev_center_name].fillna(Fields.missing)
    df[Fields.ora_employee_first_name] = df[Fields.ora_employee_first_name].fillna(Fields.missing)
    df[Fields.ora_employee_last_name] = df[Fields.ora_employee_last_name].fillna(Fields.missing)
    df[Fields.ora_menu_item_name] = df[Fields.ora_menu_item_name].fillna(Fields.missing)
    df[Fields.ora_menu_item_master_name] = df[Fields.ora_menu_item_master_name].fillna(Fields.missing)
    df[Fields.ora_major_group_name] = df[Fields.ora_major_group_name].fillna(Fields.missing)
    df[Fields.ora_family_group_name] = df[Fields.ora_family_group_name].fillna(Fields.missing)
    df[Fields.ora_location_id] = df[Fields.ora_location_id].fillna(0)
    df[Fields.ora_rev_center_id] = df[Fields.ora_rev_center_id].fillna(0)
    df[Fields.ora_employee_id] = df[Fields.ora_employee_id].fillna(0)
    df[Fields.ora_menu_item_id] = df[Fields.ora_menu_item_id].fillna(0)
    df[Fields.ora_sales_total] = df[Fields.ora_sales_total].fillna(0)
    df[Fields.ora_sales_count] = df[Fields.ora_sales_count].fillna(0)
    df[Fields.ora_sales_gross_before_discount] = df[Fields.ora_sales_gross_before_discount].fillna(0)
    df[Fields.ora_discount_total] = df[Fields.ora_discount_total].fillna(0)

    return df


def trunc_staging(bronze_df, staging_df):
    dates = bronze_df[Fields.date].unique()
    mask = staging_df[Fields.date].isin(dates)
    trunc_df = staging_df[~mask]

    return trunc_df




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

    # write
    write_parquet(
        dataframe=df,
        container=Storage.bronze,
        file_path=Storage.bronze_oracle_employees,
    )

    # delete files from blob
    files = list(staging_df[Fields.filename].unique())
    #delete_staging_files(files)


if __name__ == "__main__":
    main()
