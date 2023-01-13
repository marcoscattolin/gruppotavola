import pandas as pd
from grptavutils import (
    list_blob_files, read_parquet, read_csv, write_parquet,
    delete_blob_file, delete_staging_files, read_excel

)
from grptavutils.constants import Fields, Storage
from grptavutils.logs import logger

def read_bronze():
    try:
        df = read_parquet(Storage.bronze, Storage.bronze_oracle_discounts)
        return df

    except FileNotFoundError:
        empty_df = pd.DataFrame(columns=[
            Fields.filename,
            Fields.ora_discount_daily_total_id,
            Fields.ora_location_id,
            Fields.ora_revenue_center_id,
            Fields.ora_discount_id,
            Fields.ora_discount_total,
            Fields.ora_discount_count,
            Fields.ora_discount_gross_vat,
            Fields.ora_discount_num,
            Fields.ora_discount_name,
            Fields.ora_discount_master_num,
            Fields.ora_discount_master_name,
            Fields.ora_revenue_center_num,
            Fields.ora_revenue_center_name,
            Fields.ora_revenue_center_master_num,
            Fields.ora_revenue_center_master_name,
            Fields.ora_location_ref,
            Fields.ora_location_name,
            Fields.ora_business_date,
        ])

        return empty_df


def read_staging():
    files = list_blob_files(container_name=Storage.staging, blob_path="oracle/Discount")

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
        "discountDailyTotalID": Fields.ora_discount_daily_total_id,
        "locationID": Fields.ora_location_id,
        "revenueCenterID": Fields.ora_revenue_center_id,
        "discountID": Fields.ora_discount_id,
        "discountTotal": Fields.ora_discount_total,
        "discountCount": Fields.ora_discount_count,
        "discountGrossVat": Fields.ora_discount_gross_vat,
        "discountNum": Fields.ora_discount_num,
        "discountName": Fields.ora_discount_name,
        "discountMasterNum": Fields.ora_discount_master_num,
        "discountMasterName": Fields.ora_discount_master_name,
        "revenueCenterNum": Fields.ora_revenue_center_num,
        "revenueCenterName": Fields.ora_revenue_center_name,
        "revenueCenterMasterNum": Fields.ora_revenue_center_master_num,
        "revenueCenterMasterName": Fields.ora_revenue_center_master_name,
        "locationRef": Fields.ora_location_ref,
        "locationName": Fields.ora_location_name,
    }
    df = df.rename(columns=ren_cols)

    # select
    sel = list(ren_cols.values())
    df = df[sel]

    # data types
    df[Fields.date] = pd.to_datetime(df[Fields.date], format="%Y-%m-%d")
    df[Fields.ora_discount_daily_total_id] = df[Fields.ora_discount_daily_total_id].astype("int", errors = "ignore")
    df[Fields.ora_location_id] = df[Fields.ora_location_id].astype("int", errors = "ignore")
    df[Fields.ora_revenue_center_id] = df[Fields.ora_revenue_center_id].astype("int", errors = "ignore")
    df[Fields.ora_discount_id] = df[Fields.ora_discount_id].astype("int", errors = "ignore")
    df[Fields.ora_discount_total] = df[Fields.ora_discount_total].astype("double", errors = "ignore")
    df[Fields.ora_discount_count] = df[Fields.ora_discount_count].astype("int", errors = "ignore")
    df[Fields.ora_discount_gross_vat] = df[Fields.ora_discount_gross_vat].astype("double", errors = "ignore")
    df[Fields.ora_discount_num] = df[Fields.ora_discount_num].astype("int", errors = "ignore")
    df[Fields.ora_discount_master_num] = df[Fields.ora_discount_master_num].astype("int", errors = "ignore")
    df[Fields.ora_revenue_center_num] = df[Fields.ora_revenue_center_num].astype("int", errors = "ignore")
    df[Fields.ora_revenue_center_master_num] = df[Fields.ora_revenue_center_master_num].astype("int", errors = "ignore")

    # initcap
    df[Fields.ora_discount_name] = df[Fields.ora_discount_name].str.capitalize()
    df[Fields.ora_discount_master_name] = df[Fields.ora_discount_master_name].str.capitalize()
    df[Fields.ora_revenue_center_name] = df[Fields.ora_revenue_center_name].str.capitalize()
    df[Fields.ora_revenue_center_master_name] = df[Fields.ora_revenue_center_master_name].str.capitalize()
    df[Fields.ora_location_name] = df[Fields.ora_location_name].str.capitalize()

    # fill missing
    df[Fields.ora_discount_daily_total_id] = df[Fields.ora_discount_daily_total_id].fillna(0)
    df[Fields.ora_location_id] = df[Fields.ora_location_id].fillna(0)
    df[Fields.ora_revenue_center_id] = df[Fields.ora_revenue_center_id].fillna(0)
    df[Fields.ora_discount_id] = df[Fields.ora_discount_id].fillna(0)
    df[Fields.ora_discount_total] = df[Fields.ora_discount_total].fillna(0)
    df[Fields.ora_discount_count] = df[Fields.ora_discount_count].fillna(0)
    df[Fields.ora_discount_gross_vat] = df[Fields.ora_discount_gross_vat].fillna(0)
    df[Fields.ora_discount_num] = df[Fields.ora_discount_num].fillna(0)
    df[Fields.ora_discount_master_num] = df[Fields.ora_discount_master_num].fillna(0)
    df[Fields.ora_revenue_center_num] = df[Fields.ora_revenue_center_num].fillna(0)
    df[Fields.ora_revenue_center_master_num] = df[Fields.ora_revenue_center_master_num].fillna(0)

    df[Fields.ora_discount_name] = df[Fields.ora_discount_name].fillna(Fields.missing)
    df[Fields.ora_discount_master_name] = df[Fields.ora_discount_master_name].fillna(Fields.missing)
    df[Fields.ora_revenue_center_name] = df[Fields.ora_revenue_center_name].fillna(Fields.missing)
    df[Fields.ora_revenue_center_master_name] = df[Fields.ora_revenue_center_master_name].fillna(Fields.missing)
    df[Fields.ora_location_name] = df[Fields.ora_location_name].fillna(Fields.missing)

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
        file_path=Storage.bronze_oracle_discounts,
    )

    # delete files from blob
    files = list(staging_df[Fields.filename].unique())
    #delete_staging_files(files)


if __name__ == "__main__":
    main()
