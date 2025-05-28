import polars as pl
from hotel_price_absorber_src.database.sqlite import HotelPriceDB

def get_group_dataframe_raw(group_name: str,
                            remove_duplecates: bool = True) -> pl.DataFrame:
    """
    Retrieve a DataFrame of hotel prices for a specific group from the SQLite database.
    Raw means that it does not apply any date filtering or any type transformation.
    This function fetches all hotel prices for the specified group and optionally removes
    duplicate entries based on the latest measurement date.
    Left here for sending user raw data to the frontend as a downloadble CSV file.
    Args:
        group_name (str): The name of the group to filter the data.
        
    Returns:
        pl.DataFrame: A DataFrame containing hotel prices for the specified group.
    """
    db = HotelPriceDB()
    df = pl.DataFrame(db.get_all_by_group(group_name), infer_schema_length=None)
    
    if remove_duplecates:
        df = df.with_columns(
            pl.col("measurment_taken_at").max().over(["hotel_url", "check_in_date"]).alias("max_measurement")
        ).filter(
            pl.col("measurment_taken_at") == pl.col("max_measurement")
        ).drop("max_measurement")
    
    return df


def get_group_dataframe(group_name: str, remove_duplecates: bool = True,
                        start_date: str = None,
                        end_date: str = None) -> pl.DataFrame:
    """
    Retrieve a DataFrame of hotel prices for a specific group from the SQLite database,
    with optional filtering by date range.
    Args:
        group_name (str): The name of the group to filter the data.
        remove_duplecates (bool): Whether to remove duplicate entries based on measurement date.
        start_date (str, optional): The start date for filtering in "DD.MM.YYYY" format.
        end_date (str, optional): The end date for filtering in "DD.MM.YYYY" format.
    Returns:
        pl.DataFrame: A DataFrame containing hotel prices for the specified group,
                      filtered by date range if provided.
    """
    df = get_group_dataframe_raw(group_name, remove_duplecates)
    
        # Parse dates in Polars (DD-MM-YYYY format)
    df = df.with_columns([
        pl.col("check_in_date").str.to_date("%d-%m-%Y").alias("check_in_date")
    ])

    df = df.with_columns([
        pl.col("check_out_date").str.to_date("%d-%m-%Y").alias("check_out_date")
    ])
    
    
    filters = list()
    if start_date is not None:
        # Convert start_date from "DD.MM.YYYY" to date object
        start_date_parsed = pl.lit(start_date).str.to_date("%d.%m.%Y")
        filters.append(pl.col("check_in_date") >= start_date_parsed)
    
    if end_date is not None:
        # Convert end_date from "DD.MM.YYYY" to date object
        end_date_parsed = pl.lit(end_date).str.to_date("%d.%m.%Y")
        filters.append(pl.col("check_in_date") <= end_date_parsed)
    
    # Apply filters if any exist
    if filters:
        # Combine all filters with AND logic
        combined_filter = filters[0]
        for filter_condition in filters[1:]:
            combined_filter = combined_filter & filter_condition
        df = df.filter(combined_filter)
    
    # df = df.sort(["hotel_url", "check_in_date"])
    
    return df
    
    