from pydantic import BaseModel


class OstrovokHotelPrice(BaseModel):
    """
    Schema for Ostrovok hotel price.
    """
    hotel_name: str
    hotel_url: str | None = None
    room_name: str | None = None
    hotel_price: float
    hotel_currency: str | None = None
    hotel_rating: float | None = None
    hotel_rating_count: int | None = None
    hotel_description: str | None = None
    hotel_image_url: str | None = None
    hotel_location: str | None = None