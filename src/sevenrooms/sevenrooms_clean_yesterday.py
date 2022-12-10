import datetime

import pandas as pd
from grptavutils import list_blob_files, read_parquet, read_csv, write_parquet, delete_blob_file, delete_staging_files
from grptavutils.constants import Fields, Storage
from grptavutils.logs import logger


def read_bronze():
    try:
        df = read_parquet(Storage.bronze, Storage.yesterday_reservations)
        return df

    except FileNotFoundError:
        empty_df = pd.DataFrame(columns=[
            Fields.shift_name,
            Fields.created_date,
            Fields.created_time,
            Fields.reservation_date,
            Fields.reservation_time,
            Fields.update_date,
            Fields.update_time,
            Fields.reservation_status,
            Fields.detailed_status,
            Fields.confirmation_id,
            Fields.reservation_notes,
            Fields.booked_covers,
            Fields.reservation_tag,
            Fields.client_notes,
        ])

        return empty_df


def read_staging():
    files = list_blob_files(container_name=Storage.staging, blob_path=Storage.yesterday_reservations)

    # iterate over files
    df = pd.DataFrame()
    for f in files:
        try:
            current_df = read_parquet(container=Storage.staging, file_path=f)
            current_df.loc[:, Fields.filename] = f
            df = pd.concat([df, current_df])
        except pd.errors.EmptyDataError:
            delete_blob_file(container_name=Storage.staging, file_path=f)

    # rename columns
    ren_cols = {
        Fields.filename: Fields.filename,
        "Shift Name": Fields.shift_name,
        "Created Date": Fields.created_date,
        "Created Time": Fields.created_time,
        "Reservation Date": Fields.reservation_date,
        "Reservation Time": Fields.reservation_time,
        "Updated - Local Date": Fields.update_date,
        "Updated - Local Time": Fields.update_time,
        "Reservation Status": Fields.reservation_status,
        "Detailed Status": Fields.detailed_status,
        "Confirmation #": Fields.confirmation_id,
        "Booked By" : Fields.booked_by,
        "Reservation Notes": Fields.reservation_notes,
        "Booked Covers": Fields.booked_covers,
        "Reservation Tag Categories + Names": Fields.reservation_tag,
        "Client Notes": Fields.client_notes,
    }
    df = df.rename(columns=ren_cols)

    # select
    sel = list(ren_cols.values())
    df = df[sel]

    # data types
    df[Fields.created_date] = pd.to_datetime(df[Fields.created_date]).dt.date
    df[Fields.created_time] = pd.to_datetime(df[Fields.created_time])
    df[Fields.reservation_date] = pd.to_datetime(df[Fields.reservation_date]).dt.date

    df[Fields.update_date] = pd.to_datetime(df[Fields.update_date]).dt.date
    df[Fields.update_time] = pd.to_datetime(df[Fields.update_time])

    # replace
    df[Fields.shift_name] = df[Fields.shift_name].replace({"DINNER": "Cena", "LUNCH": "Pranzo",})

    # initcap
    df[Fields.reservation_status] = df[Fields.reservation_status].str.capitalize()
    df[Fields.detailed_status] = df[Fields.detailed_status].str.capitalize()
    df[Fields.reservation_notes] = df[Fields.reservation_notes].str.capitalize()
    df[Fields.reservation_tag] = df[Fields.reservation_tag].str.capitalize()
    df[Fields.client_notes] = df[Fields.client_notes].str.capitalize()
    df[Fields.booked_by] = df[Fields.booked_by].str.capitalize()

    # nums
    df[Fields.booked_covers] = df[Fields.booked_covers].astype("int")

    # fill missing
    df[Fields.booked_covers] = df[Fields.booked_covers].fillna(0)

    return df


def trunc_staging(bronze_df, staging_df):

    df_out = pd.concat([
        bronze_df, staging_df
    ])

    df_out = (
        df_out
        .sort_values(Fields.update_time)
        .drop_duplicates(subset=[Fields.confirmation_id], keep="last")
    )

    # susbet to before yesterday
    yesterday = datetime.date.today() - datetime.timedelta(1)
    mask = df_out[Fields.reservation_date] <= yesterday
    df_out = df_out[mask]

    return df_out

def main():

    # read bronze
    bronze_df = read_bronze()

    # read staging
    staging_df = read_staging()

    # drop form staging already available data
    trunc_df = trunc_staging(bronze_df=bronze_df, staging_df=staging_df)

    # write
    write_parquet(
        dataframe=trunc_df,
        container=Storage.bronze,
        file_path=Storage.bronze_yesterday_reservations,
    )

    # delete files from blob
    files = list(staging_df[Fields.filename].unique())
    # delete_staging_files(files)


if __name__ == "__main__":
    main()