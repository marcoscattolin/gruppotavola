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

    dates = pd.concat(datasets)
    dates = dates[[Fields.date]]
    dates = dates.drop_duplicates()

    return dates

def combine_employees(datasets):

    employees_datasets = []
    for d in datasets:
        if Fields.ora_employee_id in d.columns:
            employees_datasets.append(d)

    employees_df = pd.concat(employees_datasets)
    employees_df = employees_df[[Fields.ora_employee_id, Fields.ora_employee_first_name, Fields.ora_employee_last_name]]
    employees_df = employees_df.drop_duplicates(subset=[Fields.ora_employee_id])

    return employees_df


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

    # combine dates
    employees_df = combine_employees(datasets)

    write_parquet(
        dataframe=employees_df,
        container=Storage.silver,
        file_path=Storage.silver_employees,
    )


if __name__ == "__main__":
    main()
