import os
import sqlite3
from typing import Any, Dict, List, Optional
from sqlite3 import OperationalError

from hotel_price_absorber_src.schema import OstrovokHotelPrice

from hotel_price_absorber_src.logger import general_logger as logger

class HotelPriceDB:
    """
    Database handler for OstrovokHotelPrice data.
    Each group_name is saved to a separate table in the database.
    """
    def __init__(self):
        """Initialize the database connection using path from environment variables."""
        
        # Get path as DB_PATH/HOTEL_DB_NAME
        # db_path = os.environ.get('DB_PATH')
        db_path = os.path.join(os.environ.get('DB_PATH'), os.environ.get('HOTEL_DB_NAME'))
        
        if not db_path:
            raise ValueError("DB_PATH environment variable not set")
        
        try:
            self.conn = sqlite3.connect(db_path)
        except OperationalError as e:
            logger.error(f"Error connecting to database file {db_path}:\n {e}")
            
        self.conn.row_factory = sqlite3.Row  # This allows accessing columns by name
        
    def _get_safe_table_name(self, group_name: str) -> str:
        """Convert group_name to a safe SQL table name."""
        if not group_name:
            group_name = "default"
            
        # Replace non-alphanumeric characters with underscores
        table_name = ''.join(c if c.isalnum() else '_' for c in group_name)
        
        # Ensure table name starts with a letter
        if not table_name[0].isalpha():
            table_name = f"group_{table_name}"
            
        return f"hotel_prices_{table_name}"
    
    def _create_table_if_not_exists(self, group_name: str) -> None:
        """Create a table for the given group_name if it doesn't exist."""
        table_name = self._get_safe_table_name(group_name)
        cursor = self.conn.cursor()
        
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hotel_url TEXT NOT NULL,
            hotel_price REAL NOT NULL,
            measurment_taken_at INTEGER NOT NULL,
            check_in_date TEXT NOT NULL,
            check_out_date TEXT NOT NULL,
            hotel_name TEXT,
            hotel_currency TEXT,
            room_name TEXT,
            comments TEXT,
            group_name TEXT
        )
        ''')
        
        # Create indexes for common search fields
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_hotel_name ON {table_name} (hotel_name)")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_check_dates ON {table_name} (check_in_date, check_out_date)")
        
        self.conn.commit()
    
    def save(self, hotel_price: OstrovokHotelPrice) -> int:
        """
        Save a hotel price record to the appropriate table.
        
        Args:
            hotel_price: The hotel price data to save
            
        Returns:
            id: The ID of the inserted record
        """
        group_name = hotel_price.group_name or "default"
        self._create_table_if_not_exists(group_name)
        
        table_name = self._get_safe_table_name(group_name)
        cursor = self.conn.cursor()
        
        cursor.execute(f'''
        INSERT INTO {table_name} (
            hotel_url, hotel_price, measurment_taken_at, check_in_date, check_out_date,
            hotel_name, hotel_currency, room_name, comments, group_name
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            hotel_price.hotel_url,
            hotel_price.hotel_price,
            hotel_price.measurment_taken_at,
            hotel_price.check_in_date,
            hotel_price.check_out_date,
            hotel_price.hotel_name,
            hotel_price.hotel_currency,
            hotel_price.room_name,
            hotel_price.comments,
            hotel_price.group_name
        ))
        
        self.conn.commit()
        return cursor.lastrowid
    
    def save_batch(self, hotel_prices: List[OstrovokHotelPrice]) -> List[int]:
        """Save multiple hotel price records in batch."""
        return [self.save(hotel_price) for hotel_price in hotel_prices]
    
    def get_by_id(self, group_name: str, record_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific hotel price record by ID within a group."""
        table_name = self._get_safe_table_name(group_name)
        cursor = self.conn.cursor()
        
        # Check if table exists
        cursor.execute(f"""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name=?
        """, (table_name,))
        
        if not cursor.fetchone():
            return None
        
        cursor.execute(f"SELECT * FROM {table_name} WHERE id = ?", (record_id,))
        row = cursor.fetchone()
        
        if not row:
            return None
            
        return dict(row)
    
    def get_all_by_group(self, group_name: str) -> List[Dict[str, Any]]:
        """Get all hotel price records for a specific group."""
        table_name = self._get_safe_table_name(group_name)
        cursor = self.conn.cursor()
        
        # Check if table exists
        cursor.execute(f"""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name=?
        """, (table_name,))
        
        if not cursor.fetchone():
            return []
        
        cursor.execute(f"SELECT * FROM {table_name}")
        return [dict(row) for row in cursor.fetchall()]
    
    def search(self, group_name: str, **filters) -> List[Dict[str, Any]]:
        """
        Search for hotel price records with filters.
        
        Args:
            group_name: The group to search in
            **filters: Field filters (e.g. hotel_name='Hotel A')
            
        Returns:
            List of matching records
        """
        if not filters:
            return self.get_all_by_group(group_name)
            
        table_name = self._get_safe_table_name(group_name)
        cursor = self.conn.cursor()
        
        # Check if table exists
        cursor.execute(f"""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name=?
        """, (table_name,))
        
        if not cursor.fetchone():
            return []
            
        # Build the query dynamically
        where_clauses = []
        params = []
        
        for key, value in filters.items():
            where_clauses.append(f"{key} = ?")
            params.append(value)
            
        where_clause = " AND ".join(where_clauses)
        query = f"SELECT * FROM {table_name} WHERE {where_clause}"
        
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]
    
    def update(self, group_name: str, record_id: int, **fields_to_update) -> bool:
        """Update a hotel price record."""
        if not fields_to_update:
            return False
            
        table_name = self._get_safe_table_name(group_name)
        cursor = self.conn.cursor()
        
        # Check if table exists
        cursor.execute(f"""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name=?
        """, (table_name,))
        
        if not cursor.fetchone():
            return False
        
        # Build update statement
        set_clauses = [f"{field} = ?" for field in fields_to_update.keys()]
        set_clause = ", ".join(set_clauses)
        
        params = list(fields_to_update.values())
        params.append(record_id)
        
        query = f"UPDATE {table_name} SET {set_clause} WHERE id = ?"
        cursor.execute(query, params)
        self.conn.commit()
        
        return cursor.rowcount > 0
    
    def delete(self, group_name: str, record_id: int) -> bool:
        """Delete a hotel price record."""
        table_name = self._get_safe_table_name(group_name)
        cursor = self.conn.cursor()
        
        # Check if table exists
        cursor.execute(f"""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name=?
        """, (table_name,))
        
        if not cursor.fetchone():
            return False
            
        cursor.execute(f"DELETE FROM {table_name} WHERE id = ?", (record_id,))
        self.conn.commit()
        
        return cursor.rowcount > 0
    

    def delete_batch(self, group_name: str, record_ids: List[int]) -> bool:
        """Delete multiple hotel price records."""
        if not record_ids:
            return False
            
        table_name = self._get_safe_table_name(group_name)
        cursor = self.conn.cursor()
        
        # Check if table exists
        cursor.execute(f"""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name=?
        """, (table_name,))
        
        if not cursor.fetchone():
            return False
            
        placeholders = ', '.join('?' for _ in record_ids)
        cursor.execute(f"DELETE FROM {table_name} WHERE id IN ({placeholders})", record_ids)
        self.conn.commit()
        
        return cursor.rowcount > 0    

    def get_all_groups(self) -> List[str]:
        """Get all group names that have tables in the database."""
        cursor = self.conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'hotel_prices_%'")
        tables = [row[0] for row in cursor.fetchall()]
        
        groups = []
        for table in tables:
            cursor.execute(f"SELECT DISTINCT group_name FROM {table}")
            for row in cursor.fetchall():
                if row[0] and row[0] not in groups:
                    groups.append(row[0])
        
        return groups
    
    def get_stats(self, group_name: str) -> Dict[str, Any]:
        """Get price statistics for a group."""
        table_name = self._get_safe_table_name(group_name)
        cursor = self.conn.cursor()
        
        # Check if table exists
        cursor.execute(f"""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name=?
        """, (table_name,))
        
        if not cursor.fetchone():
            return {"count": 0, "min_price": None, "max_price": None, "avg_price": None}
        
        cursor.execute(f"""
        SELECT 
            COUNT(*) as count,
            MIN(hotel_price) as min_price,
            MAX(hotel_price) as max_price,
            AVG(hotel_price) as avg_price
        FROM {table_name}
        """)
        
        return dict(cursor.fetchone())
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            
    def __enter__(self):
        """Support for context manager."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close connection when exiting context manager."""
        self.close()