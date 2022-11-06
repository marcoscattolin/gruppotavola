import pandas as pd
from grptavutils import list_blob_files, read_parquet, read_csv, write_parquet, delete_staging_files
from grptavutils.constants import Fields, Storage
from grptavutils.logs import logger

def read_bronze():
    try:
        df = read_parquet(Storage.bronze, Storage.bronze_ga)
        return df

    except FileNotFoundError:
        empty_df = pd.DataFrame(columns=[
            Fields.filename,
            Fields.date,
            Fields.ga_source,
            Fields.ga_channel_grouping,
            Fields.ga_sessions,
            Fields.ga_users,
        ])

        return empty_df


def read_staging():
    files = list_blob_files(container_name="staging", blob_path="googleanalytics/")

    # iterate over files
    df = pd.DataFrame()
    for f in files:
        current_df = read_csv(container=Storage.staging, file_path=f)
        current_df.loc[:, Fields.filename] = f
        df = pd.concat([df, current_df])

    # rename columns
    ren_cols = {
        Fields.filename: Fields.filename,
        "date": Fields.date,
        "source": Fields.ga_source,
        "channelGrouping": Fields.ga_channel_grouping,
        "sessions": Fields.ga_sessions,
        "users": Fields.ga_users,
    }
    df = df.rename(columns=ren_cols)

    # select
    sel = list(ren_cols.values())
    df = df[sel]

    # data types
    df[Fields.date] = pd.to_datetime(df[Fields.date], format="%Y%m%d")
    df[Fields.ga_sessions] = df[Fields.ga_sessions].astype("int", errors="ignore")
    df[Fields.ga_users] = df[Fields.ga_users].astype("int", errors="ignore")

    # fill missing
    df[Fields.ga_source] = df[Fields.ga_source].fillna(Fields.missing)
    df[Fields.ga_channel_grouping] = df[Fields.ga_channel_grouping].fillna(Fields.missing)
    df[Fields.ga_sessions] = df[Fields.ga_sessions].fillna(0)
    df[Fields.ga_users] = df[Fields.ga_users].fillna(0)

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
        file_path=Storage.bronze_ga,
    )

    # delete files from blob
    files = list(staging_df[Fields.filename].unique())
    #delete_staging_files(files)


if __name__ == "__main__":
    main()
