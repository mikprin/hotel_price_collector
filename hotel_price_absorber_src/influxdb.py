from datetime import datetime, timedelta

import pandas as pd
from influxdb_client import BucketRetentionRules, InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS, WritePrecision


class HotelDataInfluxClient:
    """
    Client for interacting with InfluxDB for hotel booking data.
    
    Works with data structure from OstrovokHotelPrice pydantic model:
    {
        "hotel_url": str,             # tag - Link where prices were scraped
        "hotel_price": float,         # field
        "measurment_taken_at": int,   # timestamp
        "check_in_date": str,         # field - Format "DD-MM-YYYY"
        "check_out_date": str,        # field - Format "DD-MM-YYYY"
        "hotel_name": str,            # tag
        "hotel_currency": str,        # tag
        "room_name": str,             # tag (optional)
        "comments": str,              # field (optional)
        "group_name": str,            # tag and also used as measurement name
    }
    """
    
    def __init__(self, url, token, org, bucket, auto_create_bucket=False, retention_days=365):
        """
        Initialize the InfluxDB client.
        
        Parameters:
        - url: InfluxDB server URL
        - token: Authentication token
        - org: Organization name
        - bucket: Bucket name
        - auto_create_bucket: If True, automatically create the bucket if it doesn't exist
        - retention_days: Retention period in days (if creating a new bucket)
        """
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.retention_days = retention_days
        
        try:
            # Initialize the client
            self.client = InfluxDBClient(url=url, token=token, org=org)
            
            # Get API clients
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            self.query_api = self.client.query_api()
            self.buckets_api = self.client.buckets_api()
            self.org_api = self.client.organizations_api()
            
            print(f"Successfully connected to InfluxDB at {url}")
            
            # Auto-create bucket if needed
            if auto_create_bucket and not self.bucket_exists(self.bucket):
                print(f"Bucket '{self.bucket}' does not exist. Creating...")
                self.create_bucket(self.bucket, retention_days=retention_days)
                
        except Exception as e:
            raise ConnectionError(f"Failed to connect to InfluxDB: {e}")
    
    def bucket_exists(self, bucket_name):
        """
        Check if a bucket exists in the organization.
        
        Parameters:
        - bucket_name: Name of the bucket to check
        
        Returns:
        - bool: True if the bucket exists, False otherwise
        """
        try:
            # Get all buckets in the organization
            buckets = self.buckets_api.find_buckets().buckets
            
            # Check if bucket exists by name
            for bucket in buckets:
                if bucket.name == bucket_name:
                    return True
                    
            return False
            
        except Exception as e:
            print(f"Error checking if bucket exists: {e}")
            raise
    
    def create_bucket(self, bucket_name, retention_days=365):
        """
        Create a new bucket in the organization.
        
        Parameters:
        - bucket_name: Name of the bucket to create
        - retention_days: Data retention period in days
        
        Returns:
        - bool: True if bucket was created successfully, False otherwise
        """
        try:
            # Find organization ID by name
            orgs = self.org_api.find_organizations()
            org_id = None
            
            for org in orgs:
                if org.name == self.org:
                    org_id = org.id
                    break
            
            if not org_id:
                # Create org
                org = self.org_api.create_organization(name=self.org)
                org_id = org.id
            
            # Create retention rules
            retention_rules = BucketRetentionRules(
                type="expire",
                every_seconds=retention_days * 24 * 60 * 60  # Convert days to seconds
            )
            
            # Create bucket with retention policy
            created_bucket = self.buckets_api.create_bucket(
                bucket_name=bucket_name,
                org_id=org_id,
                retention_rules=retention_rules
            )
            
            print(f"Bucket '{bucket_name}' created successfully with {retention_days} days retention")
            return True
            
        except Exception as e:
            print(f"Error creating bucket: {e}")
            return False
    
    def delete_bucket(self, bucket_name):
        """
        Delete a bucket from the organization.
        
        Parameters:
        - bucket_name: Name of the bucket to delete
        
        Returns:
        - bool: True if bucket was deleted successfully, False otherwise
        """
        try:
            # Find bucket ID by name
            buckets = self.buckets_api.find_buckets().buckets
            bucket_id = None
            
            for bucket in buckets:
                if bucket.name == bucket_name:
                    bucket_id = bucket.id
                    break
            
            if not bucket_id:
                print(f"Bucket '{bucket_name}' not found")
                return False
            
            # Delete bucket
            self.buckets_api.delete_bucket(bucket_id)
            print(f"Bucket '{bucket_name}' deleted successfully")
            return True
            
        except Exception as e:
            print(f"Error deleting bucket: {e}")
            return False
    
    def list_buckets(self):
        """
        List all buckets in the organization.
        
        Returns:
        - list: List of bucket names
        """
        try:
            buckets = self.buckets_api.find_buckets().buckets
            return [bucket.name for bucket in buckets]
            
        except Exception as e:
            print(f"Error listing buckets: {e}")
            return []
    
    def parse_date(self, date_str):
        """Parse date from string format (DD.MM.YYYY) to datetime."""
        try:
            return datetime.strptime(date_str, "%d.%m.%Y")
        except ValueError as e:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                try:
                    return datetime.strptime(date_str, "%d-%m-%Y")
                except ValueError:
                    # If the date is not in any of the expected formats, raise an error
                    raise ValueError(f"Invalid date format. Expected DD.MM.YYYY, YYYY-MM-DD, or DD-MM-YYYY, got: {date_str}. Error: {e}")

    def create_point(self, data_point):
        """
        Create an InfluxDB Point from the provided data structure.
        Uses group_name as measurement name.
        """
        # Validate required fields
        required_fields = ["check_in_date", "check_out_date", "hotel_name", 
                           "hotel_url", "hotel_currency", "hotel_price", 
                           "group_name", "measurment_taken_at"]
        
        for field in required_fields:
            if field not in data_point:
                raise ValueError(f"Missing required field: {field}")
        
        # Use group_name as measurement name
        measurement_name = data_point["group_name"]
        
        # Create a point with measurement name
        point = Point(measurement_name)
        
        # Add tags (indexed values for efficient filtering)
        point.tag("hotel_name", data_point["hotel_name"])
        point.tag("hotel_url", data_point["hotel_url"])
        point.tag("hotel_currency", data_point["hotel_currency"])
        point.tag("group_name", data_point["group_name"])
        
        # Add optional tags
        if "room_name" in data_point and data_point["room_name"]:
            point.tag("room_name", data_point["room_name"])
        
        # Add fields (actual data values)
        point.field("check_in_date", data_point["check_in_date"])
        point.field("check_out_date", data_point["check_out_date"])
        point.field("hotel_price", float(data_point["hotel_price"]))
        
        # Add optional fields
        if "comments" in data_point and data_point["comments"]:
            point.field("comments", data_point["comments"])
        
        # Set timestamp from measurment_taken_at (unix timestamp in seconds)
        timestamp_ms = int(data_point["measurment_taken_at"]) * 1000  # Convert to milliseconds if in seconds
        point.time(timestamp_ms, write_precision=WritePrecision.S)
        
        return point
    
    def write_data_point(self, data_point):
        """Write a single data point to InfluxDB using group_name as measurement."""
        try:
            # Check if bucket exists, create if not
            if not self.bucket_exists(self.bucket):
                print(f"Bucket '{self.bucket}' does not exist. Creating...")
                if not self.create_bucket(self.bucket, retention_days=self.retention_days):
                    raise ValueError(f"Failed to create bucket '{self.bucket}'")
                
            point = self.create_point(data_point)
            self.write_api.write(bucket=self.bucket, record=point)
            return True
        except Exception as e:
            print(f"Error writing data point: {e}")
            return False
    
    def write_multiple_data_points(self, data_points):
        """Write multiple data points to InfluxDB using their respective group_names as measurements."""
        try:
            # Check if bucket exists, create if not
            if not self.bucket_exists(self.bucket):
                print(f"Bucket '{self.bucket}' does not exist. Creating...")
                if not self.create_bucket(self.bucket, retention_days=self.retention_days):
                    raise ValueError(f"Failed to create bucket '{self.bucket}'")
                
            points = [self.create_point(dp) for dp in data_points]
            self.write_api.write(bucket=self.bucket, record=points)
            return True
        except Exception as e:
            print(f"Error writing multiple data points: {e}")
            return False
    
    def query_to_dataframe(self, query):
        """Execute a Flux query and convert the results to a pandas DataFrame."""
        try:
            result = self.query_api.query_data_frame(query=query, org=self.org)
            if isinstance(result, list):
                if len(result) == 0:
                    return pd.DataFrame()
                # If multiple tables, concatenate them
                return pd.concat(result)
            return result
        except Exception as e:
            print(f"Error executing query: {e}")
            return pd.DataFrame()
    
    def query_hotels_by_name(self, hotel_name, group_name, start_time="-30d"):
        """Query hotel data by hotel name within a specific group."""
        query = f'''
        from(bucket: "{self.bucket}")
          |> range(start: {start_time})
          |> filter(fn: (r) => r["_measurement"] == "{group_name}")
          |> filter(fn: (r) => r["hotel_name"] == "{hotel_name}")
        '''
        return self.query_to_dataframe(query)
    
    def query_hotels_by_price_range(self, min_price, max_price, group_name=None, currency=None, start_time="-30d"):
        """Query hotels within a specific price range within a group or across groups."""
        # Base query
        if group_name:
            # Query within a specific group
            query = f'''
            from(bucket: "{self.bucket}")
              |> range(start: {start_time})
              |> filter(fn: (r) => r["_measurement"] == "{group_name}")
              |> filter(fn: (r) => r["_field"] == "hotel_price")
              |> filter(fn: (r) => r["_value"] >= {min_price} and r["_value"] <= {max_price})
            '''
        else:
            # Query across all groups
            query = f'''
            from(bucket: "{self.bucket}")
              |> range(start: {start_time})
              |> filter(fn: (r) => r["_field"] == "hotel_price")
              |> filter(fn: (r) => r["_value"] >= {min_price} and r["_value"] <= {max_price})
            '''
        
        # Add currency filter if provided
        if currency:
            query += f'''
          |> filter(fn: (r) => r["hotel_currency"] == "{currency}")
            '''
        
        return self.query_to_dataframe(query)
    
    def query_hotels_by_check_in_date(self, check_in_date, group_name=None, start_time="-30d"):
        """Query hotels by check-in date within a group or across groups."""
        if group_name:
            # Query within a specific group
            query = f'''
            from(bucket: "{self.bucket}")
              |> range(start: {start_time})
              |> filter(fn: (r) => r["_measurement"] == "{group_name}")
              |> filter(fn: (r) => r["_field"] == "check_in_date")
              |> filter(fn: (r) => r["_value"] == "{check_in_date}")
            '''
        else:
            # Query across all groups
            query = f'''
            from(bucket: "{self.bucket}")
              |> range(start: {start_time})
              |> filter(fn: (r) => r["_field"] == "check_in_date")
              |> filter(fn: (r) => r["_value"] == "{check_in_date}")
            '''
        return self.query_to_dataframe(query)
    
    def query_hotels_by_date_range(self, start_date, end_date, group_name=None):
        """Query hotels with timestamps within a specific date range."""
        # Convert dates to RFC3339 format for InfluxDB
        start_timestamp = self.parse_date(start_date).isoformat() + "Z"
        end_timestamp = self.parse_date(end_date).isoformat() + "Z"
        
        if group_name:
            # Query within a specific group
            query = f'''
            from(bucket: "{self.bucket}")
              |> range(start: {start_timestamp}, stop: {end_timestamp})
              |> filter(fn: (r) => r["_measurement"] == "{group_name}")
            '''
        else:
            # Query across all groups
            query = f'''
            from(bucket: "{self.bucket}")
              |> range(start: {start_timestamp}, stop: {end_timestamp})
            '''
        return self.query_to_dataframe(query)
    
    def get_price_statistics(self, group_name=None, hotel_name=None, start_time="-30d"):
        """Get price statistics (min, max, mean) for hotels within a group or across groups."""
        # Base query
        if group_name:
            query = f'''
            from(bucket: "{self.bucket}")
              |> range(start: {start_time})
              |> filter(fn: (r) => r["_measurement"] == "{group_name}")
              |> filter(fn: (r) => r["_field"] == "hotel_price")
            '''
        else:
            query = f'''
            from(bucket: "{self.bucket}")
              |> range(start: {start_time})
              |> filter(fn: (r) => r["_field"] == "hotel_price")
            '''
        
        # Add hotel name filter if provided
        if hotel_name:
            query += f'''
          |> filter(fn: (r) => r["hotel_name"] == "{hotel_name}")
            '''
        
        # Add aggregations
        query += '''
          |> group()
          |> mean()
          |> yield(name: "mean")
        '''
        
        mean_df = self.query_to_dataframe(query)
        
        # Get min and max prices
        min_query = query.replace('mean()', 'min()').replace('yield(name: "mean")', 'yield(name: "min")')
        min_df = self.query_to_dataframe(min_query)
        
        max_query = query.replace('mean()', 'max()').replace('yield(name: "mean")', 'yield(name: "max")')
        max_df = self.query_to_dataframe(max_query)
        
        # Extract values
        stats = {
            "min_price": min_df["_value"].iloc[0] if not min_df.empty else None,
            "max_price": max_df["_value"].iloc[0] if not max_df.empty else None,
            "mean_price": mean_df["_value"].iloc[0] if not mean_df.empty else None,
        }
        
        return stats
    
    def list_all_groups(self):
        """Get a list of all group names (measurements) in the bucket."""
        query = f'''
        from(bucket: "{self.bucket}")
          |> range(start: -30d)
          |> group(columns: ["_measurement"])
          |> distinct(column: "_measurement")
          |> keep(columns: ["_measurement"])
        '''
        result = self.query_to_dataframe(query)
        if not result.empty and "_measurement" in result.columns:
            return result["_measurement"].tolist()
        return []
    
    def close(self):
        """Close the InfluxDB client connection."""
        self.client.close()


# Example usage
def main():
    # InfluxDB connection parameters
    url = "http://localhost:8086"
    token = "your-api-token"
    org = "your-organization"
    bucket = "hotel_data_bucket"
    
    # Initialize client with auto bucket creation
    try:
        client = HotelDataInfluxClient(
            url=url, 
            token=token, 
            org=org, 
            bucket=bucket,
            auto_create_bucket=True,  # Automatically create bucket if it doesn't exist
            retention_days=90         # Set retention policy to 90 days
        )
    except ConnectionError as e:
        print(f"Connection failed: {e}")
        return
    
    # List all available buckets
    buckets = client.list_buckets()
    print(f"Available buckets: {buckets}")
    
    # Check if our bucket exists
    if client.bucket_exists(bucket):
        print(f"Bucket '{bucket}' exists")
    else:
        # Create the bucket if it doesn't exist
        print(f"Creating bucket '{bucket}'...")
        client.create_bucket(bucket, retention_days=90)
    
    # Sample data point based on OstrovokHotelPrice schema
    sample_data_point = {
        "hotel_url": "https://ostrovok.ru/hotel/russia/sochi/mid9226618/grant_3/?q=2042&dates=20.05.2025-21.05.2025&guests=2",
        "hotel_price": 100.0,
        "measurment_taken_at": int(datetime.now().timestamp()),  # Current unix timestamp
        "check_in_date": "20-05-2025",
        "check_out_date": "21-05-2025",
        "hotel_name": "Grand Hotel Example",
        "hotel_currency": "USD",
        "room_name": "Deluxe Suite",
        "comments": "Sample data",
        "group_name": "sochi_hotels_may_2025"
    }
    
    # Write a single data point
    success = client.write_data_point(sample_data_point)
    if success:
        print("Data point written successfully")
    
    # List all available groups (measurements)
    groups = client.list_all_groups()
    print(f"Available groups: {groups}")
    
    # Query hotels by name in specific group
    hotels = client.query_hotels_by_name("Grand Hotel Example", "sochi_hotels_may_2025")
    print(f"Found {len(hotels)} matching hotels in the specified group")
    
    # Close the connection
    client.close()


if __name__ == "__main__":
    main()