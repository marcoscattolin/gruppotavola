class Fields:

    # datasets columns

    # generic
    filename = "filename"
    date = "date"

    # google analytics
    ga_source = "ga_source"
    ga_channel_grouping = "ga_channel_grouping"
    ga_sessions = "ga_sessions"
    ga_users = "ga_users"

    missing = "_missing_"

class Storage:

    # container names
    staging = "staging"
    bronze = "bronze"
    account_name = "gruppotavolastorage"

    # filepaths
    bronze_ga = "googleanalytics/googleanalytics.parquet"