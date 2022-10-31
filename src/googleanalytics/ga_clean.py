import pandas as pd
from grptavutils import list_blob_files, read_parquet, read_csv, write_parquet, delete_blob_file


def read_bronze():
    try:
        df = read_parquet("bronze", "googleanalytics/googleanalytics.parquet")
        return df

    except FileNotFoundError:
        empty_df = pd.DataFrame(columns=[
            "filename",
            "date",
            "ga_source",
            "ga_channel_grouping",
            "ga_sessions",
            "ga_users",
        ])

        return empty_df


def read_staging():
    files = list_blob_files(container_name="staging", blob_path="googleanalytics/")

    # iterate over files
    df = pd.DataFrame()
    for f in files:
        current_df = read_csv("staging", f)
        current_df.loc[:, "filename"] = f
        df = pd.concat([df, current_df])

    # rename columns
    df = df.rename(columns={
        "filename": "filename",
        "date": "date",
        "source": "ga_source",
        "channelGrouping": "ga_channel_grouping",
        "sessions": "ga_sessions",
        "users": "ga_users",
    })

    # data types
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
    df["ga_sessions"] = df["ga_sessions"].astype("int", errors="ignore")
    df["ga_users"] = df["ga_users"].astype("int", errors="ignore")

    # fill missing
    df["ga_source"] = df["ga_source"].fillna("_missing_")
    df["ga_channel_grouping"] = df["ga_channel_grouping"].fillna("_missing_")
    df["ga_sessions"] = df["ga_sessions"].fillna(0)
    df["ga_users"] = df["ga_users"].fillna(0)

    return df


def trunc_staging(bronze_df, staging_df):
    dates = bronze_df["date"].unique()
    mask = staging_df["date"].isin(dates)
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
        trunc_df.drop(columns="filename")
    ])

    # write
    write_parquet(
        dataframe=df,
        container="bronze",
        file_path="googleanalytics/googleanalytics.parquet",
    )

    # delete files from blob
    delete_files = list(trunc_df["filename"].unique())

    # delete file
    for f in delete_files:
        delete_blob_file(container_name="bronze", file_path="googleanalytics/googleanalytics.parquet")
        print(f"Deleted {f}")


if __name__ == "__main__":
    main()
