import hashlib
import random
import re
import time
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from hotel_price_absorber_src.database.redis import PriceRange, RedisStorage
from hotel_price_absorber_src.database.user_database import HotelGroup, HotelLink, UserDataStorage
from hotel_price_absorber_src.date_utils import validate_date_range
from hotel_price_absorber_src.tasks import get_price_range_for_group

# Initialize the storage
storage = UserDataStorage()
redis_storage = RedisStorage()

# Set page config
st.set_page_config(
    page_title="Hotel Price Monitor",
    page_icon="📊",
    layout="wide"
)


# Function to extract start and end dates from range string
def extract_dates(date_range: str) -> tuple:
    """Extract start and end dates from format dd.mm.yyyy-dd.mm.yyyy"""
    start_str, end_str = date_range.split("-")
    return start_str, end_str

def get_date_range(group_name: str, date_range: str, days_of_stay: int):
    
    start_date, end_date = extract_dates(date_range)
    return PriceRange(
    created_at=int(time.time()),
    group_name=group_name,
    start_date=start_date,
    end_date=end_date,
    days_of_stay=days_of_stay)

# Function to add a new price range
def add_price_range(group_name: str, date_range: str, days_of_stay: int) -> bool:
    """Add a new price range for a group"""
    if not validate_date_range(date_range):
        return False
    
    price_range = get_date_range(group_name, date_range, days_of_stay)
    return redis_storage.add_price_range(price_range)

# Function to load hotel groups
def load_hotel_groups():
    user_data = storage.get_all_data()
    return user_data.groups

# Function to save a hotel to a group
def add_hotel_to_group(group_name: str, hotel_url: str, name: str | None = None):
    hotel = HotelLink(url=hotel_url, name=name)
    return storage.add_hotel_to_group(group_name, hotel)

# Function to remove a hotel from a group
def remove_hotel_from_group(group_name: str, hotel_url: str):
    return storage.remove_hotel_from_group(group_name, hotel_url)

# Function to add a new group
def add_new_group(group_name: str,  description: str | None = None):
    group = HotelGroup(group_name=group_name,hotels=[], description=description)
    return storage.add_group(group)

# Function to delete a group
def delete_group(group_name):
    return storage.delete_group(group_name)

# Function to generate mock price history data
def generate_price_data(hotel_url, days=30):
    dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days)]
    
    # Generate a base price from the URL (just for demo purposes)
    hash_object = hashlib.md5(hotel_url.encode())
    hex_dig = hash_object.hexdigest()
    # Convert first 4 chars of hex to a number between 50 and 500
    base_price = 50 + (int(hex_dig[:4], 16) % 450)

    # Generate price fluctuations
    prices = [base_price]
    for i in range(1, days):
        # Random price change between -5% and +5%
        change = random.uniform(-0.05, 0.05)
        new_price = prices[-1] * (1 + change)
        prices.append(new_price)
    
    # Reverse to get chronological order
    prices.reverse()
    
    return pd.DataFrame({
        'date': dates,
        'price': prices
    })

# Main app
st.title("Hotel Price Monitor")

# Create main tabs
tab1, tab2, tab3 = st.tabs(["Manage Links", "Price Ranges", "Price Analytics"])

# Tab 1: Manage Links
with tab1:
    st.header("Группы отелей")
    
    # Get all hotel groups
    groups = load_hotel_groups()
    group_names = [group.group_name for group in groups]
    
    # Add new group section
    with st.expander("Add New Hotel Group"):
        with st.form("new_group_form"):
            new_group_name = st.text_input("Group Name")
            description = st.text_area("Description", placeholder="Optional description for the group")
            location = st.text_input("Location", placeholder="Optional location of the hotels in this group")
            submitted = st.form_submit_button("Add Group")
            if submitted and new_group_name:
                if add_new_group(new_group_name, description, location):
                    st.success(f"Added group: {new_group_name}")
                    st.rerun()
                else:
                    st.error(f"Group with name '{new_group_name}' already exists")
    
    # If no groups exist yet
    if not group_names:
        st.info("No hotel groups added yet. Please add a group above.")
    else:
        # Create subtabs for each group
        subtabs = st.tabs(group_names)
        
        # For each group, create a tab with its hotels
        for i, group in enumerate(groups):
            with subtabs[i]:
                st.subheader(f"Hotels in {group.group_name}")
                
                if group.description:
                    st.markdown(f"**Group description:** {group.description}")
                
                if group.location:
                    st.markdown(f"**Group location:** {group.location}")
                
                # Show delete group button
                if st.button(f"Delete Group: {group.group_name}", key=f"delete_{group.group_name}"):
                    if delete_group(group.group_name):
                        st.success(f"Deleted group: {group.group_name}")
                        st.rerun()
                    else:
                        st.error(f"Failed to delete group: {group.group_name}")
                
                # Display hotels in this group
                if not group.hotels:
                    st.info(f"No hotels in {group.group_name} yet. Add your first hotel below.")
                else:
                    # Convert list of HotelLink objects to DataFrame
                    hotels_data = []
                    for hotel in group.hotels:
                        if hotel.name is not None:
                            hotel_name = hotel.name
                        else:
                            hotel_name = "Отель без названия."
                        hotels_data.append({
                            "name": hotel_name,
                            "url": hotel.url
                        })
                    df = pd.DataFrame(hotels_data)
                    
                    # Display editable table
                    edited_df = st.data_editor(
                        df,
                        column_config={
                            "url": "Hotel URL",
                        },
                        hide_index=True,
                        num_rows="dynamic",
                        key=f"editor_{group.group_name}"
                    )
                    
                    # Save button for edits
                    if st.button("Save Changes", key=f"save_{group.group_name}"):
                        # Get current URLs
                        current_urls = {hotel.url for hotel in group.hotels}
                        # Get edited URLs
                        edited_urls = set(edited_df["url"].tolist())
                        
                        # URLs to remove
                        to_remove = current_urls - edited_urls
                        # URLs to add
                        to_add = edited_urls - current_urls
                        
                        # Remove hotels
                        for url in to_remove:
                            remove_hotel_from_group(group.group_name, url)
                        
                        # Add hotels
                        for url in to_add:
                            if url:  # Skip empty URLs
                                add_hotel_to_group(group.group_name, url)
                        
                        st.success("Hotels updated successfully!")
                        st.rerun()
                
                # Add new hotel form
                with st.form(f"new_hotel_form_{group.group_name}"):
                    # Add some explanation text:
                    st.markdown("""
### Добавить новый отель в группу.\n
Ссылки лучше всего вставлять следующего вида:\n
https://ostrovok.ru/hotel/russia/sochi/mid8880139/tks_house_guest_house/?q=2042&dates=25.09.2024-27.09.2024&guests=2\n
https://ostrovok.ru/hotel/russia/sochi/mid10515790/silva_guest_house/?q=2042&dates=25.09.2024-27.09.2024&guests=2\n
Даты заменяться автоматически при расчете на диапазоны, которые вы добавите в следующей вкладке.""")
                    new_url = st.text_input("Hotel URL", key=f"new_url_{group.group_name}", placeholder="https://ostrovok.ru/hotel/...")
                    new_name = st.text_input("Hotel name", key=f"new_name_{group.group_name}", placeholder="Новый чудо отель!")
                    submitted = st.form_submit_button("Add Hotel")
                    if submitted and new_url and new_name:
                        if add_hotel_to_group(group.group_name, new_url, name=new_name):
                            st.success(f"Added hotel to {group.group_name} with name {new_name}")
                            st.rerun()
                        else:
                            st.error(f"Hotel already exists in {group.group_name} or invalid URL")

# Tab 2: Price Ranges
with tab2:
    st.header("Диапазоны дат для сбора цен")
    
    # Get all hotel groups
    groups = load_hotel_groups()
    group_names = [group.group_name for group in groups]
    
    # If no groups exist yet
    if not group_names:
        st.info("No hotel groups added yet. Please add a group in the 'Manage Links' tab.")
    else:
        # Create subtabs for each group
        subtabs = st.tabs(group_names)
        
        # For each group, create a tab with its price ranges
        for i, group_name in enumerate(group_names):
            with subtabs[i]:
                st.subheader(f"Диапазоны дат для группы {group_name}")
                
                # Add new price range form
                with st.form(f"new_range_form_{group_name}"):
                    st.markdown(f"Добавить новый диапазон дат для сбора цен для группы {group_name}.")
                    date_range = st.text_input(
                        "Диапазон дат", 
                        key=f"new_range_{group_name}",
                        placeholder="дд.мм.гггг-дд.мм.гггг (например: 01.06.2025-15.06.2025)"
                    )
                    days_of_stay = st.number_input(
                        "Длительность пребывания (дней)",
                        min_value=1,
                        max_value=30,
                        value=1,
                        key=f"days_of_stay_{group_name}"
                    )
                    submitted = st.form_submit_button("Добавить диапазон")
                    
                    if submitted:
                        if not date_range:
                            st.error("Пожалуйста, введите диапазон дат")
                        elif not validate_date_range(date_range):
                            st.error("Неверный формат диапазона дат. Используйте формат дд.мм.гггг-дд.мм.гггг и убедитесь, что конечная дата позже начальной.")
                        else:
                            if add_price_range(group_name, date_range, days_of_stay):
                                date_range = get_date_range(group_name, date_range, days_of_stay)
                                job_id = redis_storage.add_job( get_price_range_for_group, date_range)
                                st.success(f"Диапазон дат добавлен для группы {group_name}, JOB_ID: {job_id}")
                                st.rerun()
                            else:
                                st.error("Не удалось добавить диапазон дат")
                
                # Display existing price ranges
                price_ranges = redis_storage.get_price_ranges(group_name)
                
                if not price_ranges:
                    st.info(f"Для группы {group_name} пока не добавлено диапазонов дат. Добавьте первый диапазон выше.")
                else:
                    # Convert to DataFrame for display
                    ranges_data = []
                    for pr in price_ranges:
                        ranges_data.append({
                            "Диапазон дат": f"{pr.start_date}-{pr.end_date}",
                            "Длительность пребывания": pr.days_of_stay,
                            "Дата создания": datetime.fromtimestamp(pr.created_at).strftime("%Y-%m-%d %H:%M"),
                            "created_at": pr.created_at  # Hidden column for delete operation
                        })
                    
                    df = pd.DataFrame(ranges_data)
                    
                    # Display table (non-editable)
                    st.dataframe(
                        df[["Диапазон дат", "Длительность пребывания", "Дата создания"]],
                        hide_index=True
                    )
                    
                    # Add delete buttons for each range
                    if st.button(f"Удалить все диапазоны для группы {group_name}", key=f"delete_all_{group_name}"):
                        deleted = redis_storage.delete_all_price_ranges(group_name)
                        if deleted:
                            st.success(f"Удалено {deleted} диапазонов для группы {group_name}")
                            st.rerun()
                        else:
                            st.error(f"Не найдено диапазонов для группы {group_name}")
                    
                    # Individual delete buttons
                    st.write("Удалить отдельные диапазоны:")
                    for i, pr in enumerate(price_ranges):
                        range_text = f"{pr.start_date}-{pr.end_date} (Пребывание: {pr.days_of_stay} дней)"
                        if st.button(f"Удалить: {range_text}", key=f"delete_range_{group_name}_{i}"):
                            if redis_storage.delete_price_range(group_name, pr.created_at):
                                st.success(f"Диапазон удален: {range_text}")
                                st.rerun()
                            else:
                                st.error(f"Не удалось удалить диапазон")

# Tab 3: Price Analytics
with tab3:
    st.header("Анализ собранных цен")
    
    groups = load_hotel_groups()
    
    if not groups:
        st.info("No hotel groups added yet. Please add a group and hotels in the 'Manage Links' tab.")
    else:
        # Create a flat list of all hotels with their group information
        all_hotels = []
        for group in groups:
            for hotel in group.hotels:
                all_hotels.append({
                    "group_name": group.group_name,
                    "url": hotel.url
                })
        
        if not all_hotels:
            st.info("No hotels added yet. Please add hotels in the 'Manage Links' tab.")
        else:
            # Hotel selector
            hotel_urls = [f"{hotel['group_name']} - {hotel['url']}" for hotel in all_hotels]
            selected_hotel = st.selectbox("Select Hotel", hotel_urls)
            
            # Parse selection to get group name and URL
            selected_group, selected_url = selected_hotel.split(" - ", 1)
            
            # Date range selector
            date_range = st.slider("Date Range (days)", 7, 90, 30)
            
            # Get price data for selected hotel
            price_data = generate_price_data(selected_url, days=date_range)
            
            # Display statistics
            st.subheader("Price Statistics")
            col1, col2, col3, col4 = st.columns(4)
            
            current_price = price_data['price'].iloc[-1]
            min_price = price_data['price'].min()
            max_price = price_data['price'].max()
            avg_price = price_data['price'].mean()
            
            col1.metric("Current Price", f"${current_price:.2f}")
            col2.metric("Minimum Price", f"${min_price:.2f}")
            col3.metric("Maximum Price", f"${max_price:.2f}")
            col4.metric("Average Price", f"${avg_price:.2f}")
            
            # Plot price history
            st.subheader("Price History Chart")
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.plot(price_data['date'], price_data['price'], marker='o', linestyle='-')
            ax.set_title(f"{selected_url} - Price History")
            ax.set_xlabel("Date")
            ax.set_ylabel("Price ($)")
            ax.grid(True)
            
            # Show every nth label to avoid crowding
            n = max(1, len(price_data) // 10)
            plt.xticks(range(0, len(price_data), n), price_data['date'][::n], rotation=45)
            
            plt.tight_layout()
            st.pyplot(fig)
            
            # Show raw data
            with st.expander("View Raw Data / Табличные данные!"):
                st.dataframe(price_data)
            
            # Add download button for CSV
            st.download_button(
                label="Download Price Data as CSV (exel format)",
                data=price_data.to_csv(index=False).encode('utf-8'),
                file_name=f"{selected_url}_price_history.csv",
                mime="text/csv",
            )

# Footer
st.markdown("---")
st.caption("Hotel Price Monitor App - Prototype Version")