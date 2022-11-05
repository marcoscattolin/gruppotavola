import pandas as pd

from grptavutils.constants import Fields, Storage
from grptavutils import read_parquet, write_parquet

def read_files():

    files = [
        Storage.bronze_oracle_employees,
        Storage.bronze_oracle_guests,
        Storage.bronze_ga,
    ]

    datasets = []
    for f in files:
        tmp_df = read_parquet(container=Storage.bronze, file_path=f)
        datasets.append(tmp_df)

    return datasets


def combine_dates(datasets):

    for ix, d in enumerate(datasets):
        datasets[ix] = datasets[ix][[Fields.date]]

    dates = pd.concat(datasets)
    dates = dates.drop_duplicates()

    return dates

def combine_employees(datasets):
    for ix, d in enumerate(datasets):
        sel = [Fields.ora_employee_id, Fields.ora_employee_last_name, Fields.ora_employee_last_name]
        datasets[ix] = datasets[ix][sel]

    employees = pd.concat(datasets)
    employees = employees.drop_duplicates(subset=[Fields.ora_employee_id])

    return employees



def main():

    # read parquet files
    datasets = read_files()

    # combine dates
    dates_df = combine_dates(datasets)

    # write calendar
    write_parquet(
        dataframe=dates_df,
        container=Storage.silver,
        file_path=Storage.silver_dates,
    )

    write_parquet(
        dataframe=dates_df,
        container=Storage.silver,
        file_path=Storage.silver_employees,
    )

    print("here")


if __name__ == "__main__":
    main()
