import pandas as pd
from grptavutils import list_blob_files, read_parquet, read_csv, write_parquet, delete_blob_file, delete_staging_files, read_excel
from grptavutils.constants import Fields, Storage
from grptavutils.logs import logger


def read_bronze():
    try:
        df = read_parquet(Storage.bronze, Storage.bronze_oracle_employees)

        # drop flags
        for col in ["wine_weighted_flag", "bread_flag", "sides_flag", "appetizers_flag", "menu_flag"]:
            try:
                df.drop(columns=[col], inplace=True)
            except KeyError:
                continue

        return df

    except FileNotFoundError:
        empty_df = pd.DataFrame(columns=[
            Fields.filename,
            Fields.date,
            Fields.ora_location_id,
            Fields.ora_rev_center_id,
            Fields.ora_employee_id,
            Fields.ora_menu_item_id,
            Fields.ora_ora_rev_center_name,
            Fields.ora_employee_first_name,
            Fields.ora_employee_last_name,
            Fields.ora_menu_item_name,
            Fields.ora_menu_item_master_name,
            Fields.ora_major_group_name,
            Fields.ora_family_group_name,
            Fields.ora_sales_total,
            Fields.ora_sales_count,
            Fields.ora_sales_gross_before_discount,
            Fields.ora_discount_total,
        ])

        return empty_df


def read_staging():
    files = list_blob_files(container_name=Storage.staging, blob_path="oracle/Employee")

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
        "locationID": Fields.ora_location_id,
        "revenueCenterID": Fields.ora_rev_center_id,
        "employeeID": Fields.ora_employee_id,
        "menuItemID": Fields.ora_menu_item_id,
        "revenueCenterName": Fields.ora_ora_rev_center_name,
        "employeeFirstName": Fields.ora_employee_first_name,
        "employeeLastName": Fields.ora_employee_last_name,
        "menuItemName": Fields.ora_menu_item_name,
        "menuItemMasterName": Fields.ora_menu_item_master_name,
        "majorGroupName": Fields.ora_major_group_name,
        "familyGroupName": Fields.ora_family_group_name,
        "salesTotal": Fields.ora_sales_total,
        "salesCount": Fields.ora_sales_count,
        "grossSalesBeforeDiscount": Fields.ora_sales_gross_before_discount,
        "discountTotal": Fields.ora_discount_total,
    }
    df = df.rename(columns=ren_cols)

    # select
    sel = list(ren_cols.values())
    df = df[sel]

    # data types
    df[Fields.date] = pd.to_datetime(df[Fields.date], format="%Y-%m-%d")
    df[Fields.ora_location_id] = df[Fields.ora_location_id].astype("int", errors="ignore")
    df[Fields.ora_rev_center_id] = df[Fields.ora_rev_center_id].astype("int", errors="ignore")
    df[Fields.ora_employee_id] = df[Fields.ora_employee_id].astype("int", errors="ignore")
    df[Fields.ora_menu_item_id] = df[Fields.ora_menu_item_id].astype("int", errors="ignore")
    df[Fields.ora_sales_total] = df[Fields.ora_sales_total].astype("double", errors="ignore")
    df[Fields.ora_sales_count] = df[Fields.ora_sales_count].astype("int", errors="ignore")
    df[Fields.ora_sales_gross_before_discount] = df[Fields.ora_sales_gross_before_discount].astype("double",
                                                                                                   errors="ignore")
    df[Fields.ora_discount_total] = df[Fields.ora_discount_total].astype("double", errors="ignore")

    # initcap
    df[Fields.ora_ora_rev_center_name] = df[Fields.ora_ora_rev_center_name].str.capitalize()
    df[Fields.ora_employee_first_name] = df[Fields.ora_employee_first_name].str.capitalize()
    df[Fields.ora_employee_last_name] = df[Fields.ora_employee_last_name].str.capitalize()
    df[Fields.ora_menu_item_name] = df[Fields.ora_menu_item_name].fillna(Fields.missing)
    df[Fields.ora_menu_item_master_name] = df[Fields.ora_menu_item_master_name].str.capitalize()
    df[Fields.ora_major_group_name] = df[Fields.ora_major_group_name].str.capitalize()
    df[Fields.ora_family_group_name] = df[Fields.ora_family_group_name].str.capitalize()

    # fill missing
    df[Fields.ora_ora_rev_center_name] = df[Fields.ora_ora_rev_center_name].fillna(Fields.missing)
    df[Fields.ora_employee_first_name] = df[Fields.ora_employee_first_name].fillna(Fields.missing)
    df[Fields.ora_employee_last_name] = df[Fields.ora_employee_last_name].fillna(Fields.missing)
    df[Fields.ora_menu_item_name] = df[Fields.ora_menu_item_name].fillna(Fields.missing)
    df[Fields.ora_menu_item_master_name] = df[Fields.ora_menu_item_master_name].fillna(Fields.missing)
    df[Fields.ora_major_group_name] = df[Fields.ora_major_group_name].fillna(Fields.missing)
    df[Fields.ora_family_group_name] = df[Fields.ora_family_group_name].fillna(Fields.missing)
    df[Fields.ora_location_id] = df[Fields.ora_location_id].fillna(0)
    df[Fields.ora_rev_center_id] = df[Fields.ora_rev_center_id].fillna(0)
    df[Fields.ora_employee_id] = df[Fields.ora_employee_id].fillna(0)
    df[Fields.ora_menu_item_id] = df[Fields.ora_menu_item_id].fillna(0)
    df[Fields.ora_sales_total] = df[Fields.ora_sales_total].fillna(0)
    df[Fields.ora_sales_count] = df[Fields.ora_sales_count].fillna(0)
    df[Fields.ora_sales_gross_before_discount] = df[Fields.ora_sales_gross_before_discount].fillna(0)
    df[Fields.ora_discount_total] = df[Fields.ora_discount_total].fillna(0)

    return df


def trunc_staging(bronze_df, staging_df):
    dates = bronze_df[Fields.date].unique()
    mask = staging_df[Fields.date].isin(dates)
    trunc_df = staging_df[~mask]

    return trunc_df


def flag_wines(df_in: pd.DataFrame) -> pd.DataFrame:
    # identify wines
    mask = df_in["major_group_name"].str.lower().str.startswith("beverage") & (
            df_in["family_group_name"].str.lower().str.startswith("vin") | df_in[
        "family_group_name"].str.lower().str.startswith("bollicin")
    )
    wines_df = df_in.loc[mask, ["menu_item_id", "family_group_name"]]
    wines_df = wines_df.drop_duplicates(subset="menu_item_id")

    # identify glasses
    glass_mask = (df_in["family_group_name"].str.lower().str.contains("calic"))

    # make weight
    wines_df["wine_weighted_flag"] = 1
    wines_df.loc[glass_mask, "wine_weighted_flag"] = .25

    wines_df.drop(columns="family_group_name", inplace=True)
    df = df_in.merge(wines_df, on="menu_item_id", how="left")
    df["wine_weighted_flag"] = df["wine_weighted_flag"].fillna(0)

    return df


def flag_bread(df_in: pd.DataFrame) -> pd.DataFrame:
    # identify bread
    mask = df_in["family_group_name"].str.lower() == "pane"
    bread_df = df_in.loc[mask, ["menu_item_id"]]
    bread_df = bread_df.drop_duplicates(subset="menu_item_id")

    # make weight
    bread_df["bread_flag"] = 1

    # merge back
    df = df_in.merge(bread_df, on="menu_item_id", how="left")
    df["bread_flag"] = df["bread_flag"].fillna(0)

    return df


def flag_sides(df_in: pd.DataFrame) -> pd.DataFrame:
    # identify sides
    mask = df_in["family_group_name"].str.lower().str.startswith("contorni")
    sides_df = df_in.loc[mask, ["menu_item_id"]]
    sides_df = sides_df.drop_duplicates(subset="menu_item_id")

    # make weight
    sides_df["sides_flag"] = 1

    # merge back
    df = df_in.merge(sides_df, on="menu_item_id", how="left")
    df["sides_flag"] = df["sides_flag"].fillna(0)

    return df


def flag_appetizers(df_in: pd.DataFrame) -> pd.DataFrame:
    # identify appetizers
    mask = df_in["family_group_name"].str.lower().str.startswith("aperitivi")
    sides_df = df_in.loc[mask, ["menu_item_id"]]
    sides_df = sides_df.drop_duplicates(subset="menu_item_id")

    # make weight
    sides_df["appetizers_flag"] = 1

    # merge back
    df = df_in.merge(sides_df, on="menu_item_id", how="left")
    df["appetizers_flag"] = df["appetizers_flag"].fillna(0)

    return df


def flag_menus(df_in: pd.DataFrame) -> pd.DataFrame:
    # identify menus
    mask = df_in["menu_item_name"].str.lower().str.contains("menu guida")
    sides_df = df_in.loc[mask, ["menu_item_id"]]
    sides_df = sides_df.drop_duplicates(subset="menu_item_id")

    # make weight
    sides_df["menu_flag"] = 1

    # merge back
    df = df_in.merge(sides_df, on="menu_item_id", how="left")
    df["menu_flag"] = df["menu_flag"].fillna(0)

    return df


def read_item_cost():

    df = read_excel(Storage.bronze, Storage.item_cost)
    df = df.drop(columns=[Fields.ora_family_group_name, Fields.ora_menu_item_name])

    return df


def calc_margin(df_in):

    # read productivity data and make key
    item_df = read_item_cost()
    item_df = item_df[[Fields.ora_menu_item_id, Fields.menu_item_cost]]

    # join and calculate margin
    df = (
        df_in
        .merge(item_df, on=Fields.ora_menu_item_id, how="left")
    )
    df[Fields.margin] = df[Fields.sales_count] * df[Fields.menu_item_cost]

    return df


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

    # flag group of items
    df = flag_appetizers(df)
    df = flag_bread(df)
    df = flag_sides(df)
    df = flag_wines(df)
    df = flag_menus(df)

    # write
    write_parquet(
        dataframe=df,
        container=Storage.bronze,
        file_path=Storage.bronze_oracle_employees,
    )

    # delete files from blob
    files = list(staging_df[Fields.filename].unique())
    # delete_staging_files(files)


if __name__ == "__main__":
    main()
