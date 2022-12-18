import datetime
import pandas as pd
from grptavutils.constants import Fields, Storage
from grptavutils import read_parquet, write_parquet
from prophet import Prophet


def read_sales():
    df = read_parquet(container=Storage.bronze, file_path=Storage.bronze_oracle_employees)
    df = (
        df
        .groupby([Fields.date], as_index=False)
        .agg(**{
            Fields.ora_sales_total: (Fields.ora_sales_total, "sum"),
        }))

    return df



def make_forecast(df_in: pd.DataFrame):

    renaming_dict = {
        Fields.date: "ds",
        Fields.ora_sales_total: "y",
    }
    df_in.rename(columns=renaming_dict, inplace=True)

    # trunc to relevant dates
    mask = df_in["ds"] >= datetime.datetime(2022, 9, 1, 0, 0)
    df = df_in[mask].copy()

    # resample
    df = df.set_index("ds").resample("D").sum().reset_index().fillna(0)

    # make non zeroes
    df["y"] = df["y"] + .01

    fcst_model = Prophet(changepoint_prior_scale=0.001, seasonality_mode="multiplicative")
    fcst_model.fit(df)

    future = fcst_model.make_future_dataframe(periods=28)
    forecast = fcst_model.predict(future)

    # flag forecast
    forecast[Fields.fcst_observation] = "actual"
    mask = forecast["ds"] > df["ds"].max()
    forecast.loc[mask, Fields.fcst_observation] = "forecast"

    # rename columns
    forecast.rename(columns={"ds": Fields.date}, inplace=True)

    return forecast





def main():

    # read parquet files
    datasets = read_sales()

    # forecast
    fcst_df = make_forecast(datasets)


    # write forecast
    write_parquet(
        dataframe=fcst_df,
        container=Storage.bronze,
        file_path=Storage.bronze_forecast,
    )


if __name__ == "__main__":
    main()