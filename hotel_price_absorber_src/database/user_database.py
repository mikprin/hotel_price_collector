import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel


# Your existing models
class HotelLink(BaseModel):
    url: str
    name: str | None = None

class HotelGroup(BaseModel):
    """
    Schema for hotel group.
    """
    group_name: str
    hotels: list[HotelLink]
    description: str | None = None
    location: str | None = None # Location of the hotel group for the AI

class UserConfig(BaseModel):
    groups: list[HotelGroup]


class UserDataStorage:
    """
    Class to manage user data storage in a JSON file.
    Uses the environment variables DB_PATH and USER_DATA_JSON to locate the file.
    """
    
    def __init__(self):
        """Initialize the UserDataStorage with path from environment variables."""
        db_path = os.getenv("DB_PATH", "/database")
        json_filename = os.getenv("USER_DATA_JSON", "user_data.json")
        
        self.db_dir = Path(db_path)
        self.file_path = self.db_dir / json_filename
        
        # Ensure the directory exists
        self.db_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize the database if it doesn't exist
        if not self.file_path.exists():
            self._initialize_db()
    
    def _initialize_db(self) -> None:
        """Create an empty database file with the default structure."""
        default_data = UserConfig(groups=[])
        self._save_data(default_data)
    
    def _load_data(self) -> UserConfig:
        """Load data from the JSON file and validate with Pydantic model."""
        try:
            with open(self.file_path, "r") as f:
                data = json.load(f)
            return UserConfig(**data)
        except (json.JSONDecodeError, FileNotFoundError):
            # If the file is corrupted or missing, initialize it
            self._initialize_db()
            return UserConfig(groups=[])
    
    def _save_data(self, data: UserConfig) -> None:
        """Save data to the JSON file."""
        with open(self.file_path, "w") as f:
            json.dump(data.model_dump(), f, indent=2)
    
    def get_all_data(self) -> UserConfig:
        """Get all user data."""
        return self._load_data()
    
    def get_group(self, group_name: str) -> Optional[HotelGroup]:
        """Get a specific hotel group by name."""
        data = self._load_data()
        for group in data.groups:
            if group.group_name == group_name:
                return group
        return None
    
    def add_group(self, group: HotelGroup) -> bool:
        """Add a new hotel group."""
        data = self._load_data()
        
        # Check if a group with the same name already exists
        for existing_group in data.groups:
            if existing_group.group_name == group.group_name:
                return False
        
        data.groups.append(group)
        self._save_data(data)
        return True
    
    def update_group(self, group_name: str, new_group: HotelGroup) -> bool:
        """Update an existing hotel group."""
        data = self._load_data()
        
        for i, group in enumerate(data.groups):
            if group.group_name == group_name:
                data.groups[i] = new_group
                self._save_data(data)
                return True
        
        return False
    
    def delete_group(self, group_name: str) -> bool:
        """Delete a hotel group by name."""
        data = self._load_data()
        
        initial_length = len(data.groups)
        data.groups = [group for group in data.groups if group.group_name != group_name]
        
        if len(data.groups) < initial_length:
            self._save_data(data)
            return True
        
        return False
    
    def add_hotel_to_group(self, group_name: str, hotel: HotelLink) -> bool:
        """Add a hotel to a specific group."""
        data = self._load_data()
        
        for group in data.groups:
            if group.group_name == group_name:
                # Check if the hotel already exists in the group
                for existing_hotel in group.hotels:
                    if existing_hotel.url == hotel.url:
                        return False
                
                if hotel.name is None:
                    return False  # Hotel name is required
                
                group.hotels.append(hotel)
                self._save_data(data)
                return True
        
        return False
    
    def remove_hotel_from_group(self, group_name: str, hotel_url: str) -> bool:
        """Remove a hotel from a specific group by URL."""
        data = self._load_data()
        
        for group in data.groups:
            if group.group_name == group_name:
                initial_length = len(group.hotels)
                group.hotels = [hotel for hotel in group.hotels if hotel.url != hotel_url]
                
                if len(group.hotels) < initial_length:
                    self._save_data(data)
                    return True
        
        return False
    
    def get_all_groups(self) -> List[str]:
        """Get a list of all group names."""
        data = self._load_data()
        return [group.group_name for group in data.groups]
    
    def get_all_hotels_in_group(self, group_name: str) -> List[HotelLink]:
        """Get all hotels in a specific group."""
        group = self.get_group(group_name)
        if group:
            return group.hotels
        return []