import pandas as pd
from grptavutils import list_blob_files, read_parquet, read_csv, write_parquet, delete_blob_file, delete_staging_files
from grptavutils.constants import Fields, Storage
from grptavutils.logs import logger
import datetime


def read_staging():

    df = read_parquet(container=Storage.staging, file_path=Storage.future_reservations)

    # rename columns
    ren_cols = {
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

    # reference date for consistency with other datasets
    df[Fields.date] = df[Fields.reservation_date]

    return df


def main():


    # read staging
    staging_df = read_staging()

    # susbet to after yesterday
    yesterday = datetime.date.today() - datetime.timedelta(1)
    mask = staging_df[Fields.reservation_date] > yesterday
    df_out = staging_df[mask]

    # write
    write_parquet(
        dataframe=df_out,
        container=Storage.bronze,
        file_path=Storage.bronze_future_reservations,
    )



if __name__ == "__main__":
    main()