import polars as pl
from hotel_price_absorber_src.database.sqlite import HotelPriceDB

def get_group_dataframe(group_name: str, remove_duplecates = True) -> pl.DataFrame:
    """
    Retrieve a DataFrame of hotel prices for a specific group from the SQLite database.
    
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
    