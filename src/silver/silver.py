import pandas as pd

from grptavutils.constants import Fields, Storage
from grptavutils import read_parquet, write_parquet
from grptavutils.logs import logger


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
    # subset datasets to those having employees data
    employees_datasets = []
    for d in datasets:
        if Fields.ora_employee_id in d.columns:
            employees_datasets.append(d)

    # concat and drop duplicates
    sel = [Fields.ora_employee_id, Fields.ora_employee_first_name, Fields.ora_employee_last_name]
    employees_df = pd.concat(employees_datasets)
    employees_df = (
        employees_df[sel]
        .sort_values(sel)
        .drop_duplicates(subset=[Fields.ora_employee_id])
    )

    return employees_df


def combine_items(datasets):
    # subset datasets to those having items data
    items_datasets = []
    for d in datasets:
        if Fields.ora_menu_item_id in d.columns:
            items_datasets.append(d)

    # concat and drop duplicates
    sel = [Fields.ora_menu_item_id, Fields.ora_menu_item_name, Fields.ora_menu_item_master_name,
           Fields.ora_major_group_name, Fields.ora_family_group_name, ]
    items_df = pd.concat(items_datasets)
    items_df = (
        items_df[sel]
        .sort_values(sel)
        .drop_duplicates(subset=[Fields.ora_menu_item_id])
    )

    return items_df


def main():
    # read parquet files
    datasets = read_files()

    # combine dates
    dates_df = combine_dates(datasets)
    write_parquet(
        dataframe=dates_df,
        container=Storage.silver,
        file_path=Storage.silver_dates,
    )

    # combine employees
    employees_df = combine_employees(datasets)
    write_parquet(
        dataframe=employees_df,
        container=Storage.silver,
        file_path=Storage.silver_employees,
    )

    # combine items
    items_df = combine_items(datasets)
    write_parquet(
        dataframe=items_df,
        container=Storage.silver,
        file_path=Storage.silver_items,
    )


if __name__ == "__main__":
    main()
