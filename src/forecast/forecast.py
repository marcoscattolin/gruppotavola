import datetime

import numpy as np
import pandas as pd

from grptavutils.constants import Fields, Storage
from grptavutils import read_parquet, write_parquet

import matplotlib.pyplot as plt

from prophet import Prophet


def read_sales():
    df = read_parquet(container=Storage.bronze, file_path=Storage.bronze_oracle_employees)
    df = (
        df
        .groupby([Fields.date], as_index=False)
        .agg(**{
            Fields.ora_sales_total: (Fields.ora_sales_total, "sum"),
        }))

    renaming_dict = {
        Fields.date: "ds",
        Fields.ora_sales_total: "y",
    }
    df.rename(columns=renaming_dict, inplace=True)

    return df


def read_bookings():

    # read bookings data
    past_df = read_parquet(container=Storage.bronze, file_path=Storage.bronze_yesterday_reservations)
    future_df = read_parquet(container=Storage.bronze, file_path=Storage.bronze_future_reservations)
    df = pd.concat([past_df, future_df])

    # calculate anticipation and susbet to lines relevant for forecasting
    df[Fields.reservation_time] = df[Fields.reservation_date].map(str) + " " + df[Fields.reservation_time].map(str) + ":00"
    df[Fields.reservation_date] = pd.to_datetime(df[Fields.reservation_date])
    df["_anticipation"] = (df[Fields.reservation_date] - df[Fields.created_time])/np.timedelta64(1, "D")
    mask = df["_anticipation"] >= 1
    df = df[mask]

    df = df.groupby(Fields.reservation_date, as_index=False).size().rename(columns={Fields.reservation_date: "ds", "size": "_bookings"})
    df = df.set_index("ds").resample("D").sum().reset_index()

    return df


def make_forecast(sales_in: pd.DataFrame, book_in: pd.DataFrame):

    train_df = sales_in.merge(book_in, on="ds", how="left")

    # trunc to relevant dates (1st nov is when we started tracked bookings
    mask = train_df["ds"] >= datetime.datetime(2022, 11, 1, 0, 0)
    train_df = train_df[mask].copy()

    # resample
    train_df = train_df.set_index("ds").resample("D").sum().reset_index().fillna(0)

    # fit model
    fcst_model = (
        Prophet(changepoint_prior_scale=0.001, seasonality_mode="multiplicative")
        .add_regressor("_bookings")
        .fit(train_df)
    )

    # forecast
    future_df = (
        fcst_model
        .make_future_dataframe(periods=28)
        .merge(book_in, on="ds")
    )
    mask = future_df["ds"] >= datetime.datetime(2022, 11, 1, 0, 0)
    future_df = future_df[mask].copy()

    # resample
    future_df = future_df.set_index("ds").resample("D").sum().reset_index().fillna(0)

    # predict
    forecast = fcst_model.predict(future_df)

    # flag forecast
    forecast[Fields.fcst_observation] = "actual"
    mask = forecast["ds"] > train_df["ds"].max()
    forecast.loc[mask, Fields.fcst_observation] = "forecast"

    # rename columns
    forecast.rename(columns={"ds": Fields.date}, inplace=True)

    return forecast


def main():

    # read parquet files
    sales_df = read_sales()
    book_df = read_bookings()

    # forecast
    fcst_df = make_forecast(sales_in=sales_df, book_in=book_df)

    # write forecast
    write_parquet(
        dataframe=fcst_df,
        container=Storage.bronze,
        file_path=Storage.bronze_forecast,
    )


if __name__ == "__main__":
    main()
