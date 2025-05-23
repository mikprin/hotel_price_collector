import json
import os
import time
from typing import Any, Dict, List, Optional

import redis
from pydantic import BaseModel
from rq import Queue
from rq.job import Job

from hotel_price_absorber_src.logger import general_logger as logger


class PriceRange(BaseModel):
    """Used to set range in which prices are needed to be collected"""
    created_at: int  # Timestamp when request was created
    group_name: str
    start_date: str
    end_date: str
    days_of_stay: int = 1

class RedisStorage:
    """Class to manage data storage in Redis."""
    
    def __init__(self):
        """Initialize the RedisStorage with connection parameters from environment variables."""
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", 6379))
        ranges_db = int(os.getenv("RANGES_DB", 0))
        
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=ranges_db,
            decode_responses=True
        )
        
        self.redis_job_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=0,
            decode_responses=True
        )
    
    def add_price_range(self, price_range: PriceRange) -> bool:
        """Add a new price range to Redis."""
        key = f"price_range:{price_range.group_name}:{price_range.created_at}"
        try:
            self.redis_client.hset(
                key,
                mapping={
                    "created_at": price_range.created_at,
                    "group_name": price_range.group_name,
                    "start_date": price_range.start_date,
                    "end_date": price_range.end_date,
                    "days_of_stay": price_range.days_of_stay
                }
            )
            return True
        except Exception as e:
            print(f"Error adding price range: {e}")
            return False
    
    def get_price_ranges(self, group_name: Optional[str] = None) -> List[PriceRange]:
        """
        Get all price ranges or filter by group name.
        
        Args:
            group_name: Optional group name to filter by
            
        Returns:
            List of PriceRange objects
        """
        result = []
        pattern = f"price_range:{group_name}:*" if group_name else "price_range:*"
        
        for key in self.redis_client.keys(pattern):
            data = self.redis_client.hgetall(key)
            if data:
                # Convert string values to appropriate types
                data["created_at"] = int(data["created_at"]) 
                data["days_of_stay"] = int(data["days_of_stay"])
                
                price_range = PriceRange(**data)
                result.append(price_range)
        
        # Sort by created_at timestamp
        result.sort(key=lambda x: x.created_at, reverse=True)
        return result
    
    def delete_price_range(self, group_name: str, created_at: int) -> bool:
        """Delete a price range by group name and created_at timestamp."""
        key = f"price_range:{group_name}:{created_at}"
        try:
            return bool(self.redis_client.delete(key))
        except Exception as e:
            print(f"Error deleting price range: {e}")
            return False
    
    def delete_all_price_ranges(self, group_name: str) -> int:
        """Delete all price ranges for a specific group."""
        pattern = f"price_range:{group_name}:*"
        keys = self.redis_client.keys(pattern)
        if keys:
            return self.redis_client.delete(*keys)
        return 0
    
    def add_job(self, function: str, data: Any, job_id: str | None = None, timout = 16000) -> bool:
        """Add a job to the Redis queue."""
        queue = Queue(connection=self.redis_job_client, default_timeout=timout)
        try:
            if job_id:
                job = Job.fetch(job_id, connection=self.redis_job_client)
            else:
                job = queue.enqueue(function,
                                    data,
                                    job_timeout=timout)
            return job.id
        except Exception as e:
            logger(f"Error adding job: {e}")
            return False