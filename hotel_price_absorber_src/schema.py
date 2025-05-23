from pydantic import BaseModel


class HotelPrice(BaseModel):
    """
    Schema for hotel price in general.
    """
    hotel_url: str # Link where prices where scraped
    hotel_price: float
    measurment_taken_at: int # Format timestamp
    check_in_date: str # Format "DD-MM-YYYY"
    check_out_date: str # Format "DD-MM-YYYY"
    hotel_name: str | None = None
    hotel_currency: str | None = None
    room_name: str | None = None # Optional field. In case there will be multiple rooms
    comments: str | None = None
    group_name: str | None = None # For saving series of measurments


class OstrovokHotelPrice(HotelPrice):
    """
    Schema for Ostrovok hotel price.
    """
    pass