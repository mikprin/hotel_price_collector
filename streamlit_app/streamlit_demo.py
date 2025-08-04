import hashlib
import random
import re
import time
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

import pandas as pd

# Add these imports at the top of your file
import plotly.graph_objects as go
import plotly.express as px
import polars as pl
from hotel_price_absorber_src.database.sqlite import HotelPriceDB

from uuid import uuid4

from hotel_price_absorber_src.database.redis import PriceRange, RedisStorage
from hotel_price_absorber_src.database.user_database import HotelGroup, HotelLink, UserDataStorage
from hotel_price_absorber_src.date_utils import validate_date_range
from hotel_price_absorber_src.tasks import get_price_range_for_group
from hotel_price_absorber_src.database.data_conversion import  get_group_dataframe, get_group_dataframe_raw
from hotel_price_absorber_src.logger import general_logger as logger

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
    """Get new price range from dates"""
    if not validate_date_range(date_range):
        return False
    uuid = uuid4()
    
    start_date, end_date = extract_dates(date_range)
    return PriceRange(
        created_at=int(time.time()),
        group_name=group_name,
        start_date=start_date,
        end_date=end_date,
        days_of_stay=days_of_stay,
        run_id=str(uuid),
        )

# Function to add a new price range
def add_price_range(range: PriceRange) -> bool:
    """Add a new price range for a group"""
    return redis_storage.add_price_range(range)

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
def add_new_group(group_name: str,  description: str | None = None, location : str | None = None):
    group = HotelGroup(group_name=group_name,hotels=[], description=description, location=location)
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

# Function to render manage links tab for a specific group
def render_manage_links_tab(group):
    st.header(f"Управление отелями в группе: {group.group_name}")
    
    if group.description:
        st.markdown(f"**Описание группы:** {group.description}")
    
    if group.location:
        st.markdown(f"**Локация группы:** {group.location}")
    
    # Show delete group button
    if st.button(f"Удалить группу: {group.group_name}", key=f"delete_{group.group_name}", type="secondary"):
        if delete_group(group.group_name):
            st.success(f"Группа удалена: {group.group_name}")
            st.rerun()
        else:
            st.error(f"Не удалось удалить группу: {group.group_name}")
    
    # Display hotels in this group
    if not group.hotels:
        st.info(f"В группе {group.group_name} пока нет отелей. Добавьте первый отель ниже.")
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
        if st.button("Сохранить изменения", key=f"save_{group.group_name}"):
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
                    _ = add_hotel_to_group(group.group_name, url)
            st.success("Отели успешно обновлены!")
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
        submitted = st.form_submit_button("Добавить отель")
        if submitted and new_url and new_name:
            # Check if hotel name is already in the group
            if any(hotel.name == new_name for hotel in group.hotels) or new_name == "" or new_name is None:
                st.error(f"Отель с названием '{new_name}' уже существует в группе {group.group_name}. Пожалуйста, выберите другое название.")
            elif add_hotel_to_group(group.group_name, new_url, name=new_name):
                st.success(f"Отель добавлен в группу {group.group_name} с названием {new_name}")
                st.rerun()
            else:
                st.error(f"Отель уже существует в группе {group.group_name} или неверный URL. Или нет названия отеля.")

# Function to render price ranges tab for a specific group
def render_price_ranges_tab(group_name):
    st.header(f"Диапазоны дат для группы: {group_name}")
    
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
                date_range = get_date_range(group_name, date_range, days_of_stay)
                if date_range:
                    job_id = redis_storage.add_job( get_price_range_for_group, date_range)
                    date_range.job_id = job_id
                    st.success(f"Диапазон дат добавлен для группы {group_name}, JOB_ID: {job_id}")
                    add_price_range(date_range)
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
            
            if pr.job_id:
                status = redis_storage.get_job_status(job_id=pr.job_id)

            if pr.run_id:
                run_id = pr.run_id
            else:
                run_id = ""
            
            ranges_data.append({
                "Диапазон дат": f"{pr.start_date}-{pr.end_date}",
                "Длительность пребывания": pr.days_of_stay,
                "Дата создания": datetime.fromtimestamp(pr.created_at).strftime("%Y-%m-%d %H:%M"),
                "created_at": pr.created_at,  # Hidden column for delete operation,
                "Статус": status if pr.job_id else "Неизвестно",
                "run id": run_id,
            })
        
        df = pd.DataFrame(ranges_data)
        
        # Display table (non-editable)
        st.dataframe(
            df[["Диапазон дат", "Длительность пребывания", "Дата создания", "Статус","run id"]],
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

# Function to render price analytics tab for a specific group
def render_price_analytics_tab(group):
    st.header(f"Анализ цен для группы: {group.group_name}")
    
    # Initialize database connection
    db = HotelPriceDB()
    
    try:
        # Get data for this group
        df = get_group_dataframe(group.group_name, remove_duplecates=True)
        # Check if the DataFrame is empty
        if df.is_empty():
            st.info(f"Нет данных о ценах для группы '{group.group_name}'. Добавьте диапазоны дат во вкладке 'Диапазоны дат' и дождитесь завершения сбора данных.")
            return

        # Polars dates are already in datetime ( from DD-MM-YYYY format)
        
        # Sort by check-in date
        df = df.sort("check_in_date")
        
        # Date range selection
        st.markdown(f"""Выберите диапазон дат для анализа цен в группе {group.group_name}. Выбранный диапазон повлияет на все показываемые графики и таблицы ниже.
По умолчанию показываются все доступные данные.""")
        
        col1, col2, col3 = st.columns([1, 1, 1])
        
        # Get min and max dates from the data for default values
        df_min_date = df['check_in_date'].min()
        df_max_date = df['check_in_date'].max()
        
        # Initialize start and end dates for the date inputs
        
        default_start_date = max(datetime.now().date(), df_min_date)
        # default_start_date = df_min_date
        
        # logger.info(f"Default start date: {default_start_date}, type: {type(default_start_date)}")
        start_date = default_start_date
        end_date = df_max_date

        field_min_date = min(df_min_date, default_start_date)

        # Date inputs for filtering
        with col1:
            start_date = st.date_input(
                "Начальная дата",
                value=default_start_date,
                min_value=field_min_date,
                max_value=df_max_date,
                key=f"start_date_table_{group.group_name}",
                format="DD.MM.YYYY"
            )
        
        with col2:
            end_date = st.date_input(
                "Конечная дата",
                value=df_max_date,
                min_value=df_min_date,
                max_value=df_max_date,
                key=f"end_date_table_{group.group_name}",
                format="DD.MM.YYYY"
            )
        
        with col3:
            # Reset button to show all dates
            if st.button("Показать все даты", key=f"reset_dates_{group.group_name}"):
                start_date = df_min_date
                end_date = df_max_date
                # st.rerun()
        # Filter DataFrame by selected date range
        date_filtered_df = df.filter(
            (pl.col('check_in_date') >= pl.lit(start_date)) &
            (pl.col('check_in_date') <= pl.lit(end_date))
        )
        
        # Check if the filtered DataFrame is empty
        if date_filtered_df.is_empty():
            st.warning("Нет данных в выбранном диапазоне дат. Пожалуйста, выберите другой период.")
        else:
            
            # Display basic statistics using Polars
            st.subheader("📊 Статистика по ценам")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_records = date_filtered_df.height
                st.metric("Всего записей за выбранный период", total_records)
            
            with col2:
                unique_hotels = date_filtered_df['hotel_name'].n_unique() if 'hotel_name' in df.columns else 0
                st.metric("Уникальных отелей", unique_hotels)
            
            with col3:
                if 'hotel_price' in date_filtered_df.columns:
                    avg_price = date_filtered_df['hotel_price'].mean()
                    st.metric("Средняя цена", f"₽{avg_price:,.0f}")
                else:
                    st.metric("Средняя цена", "N/A")
            
            with col4:
                if 'check_in_date' in date_filtered_df.columns:
                    min_date = date_filtered_df['check_in_date'].min()
                    max_date = date_filtered_df['check_in_date'].max()
                    date_range = f"{min_date} - {max_date}"
                    st.metric("Период данных", date_range)
                else:
                    st.metric("Период данных", "N/A")
            
            # Show data preview
            with st.expander("Просмотр всех сырых данных"):
                st.dataframe(get_group_dataframe_raw(group.group_name, remove_duplecates=True).to_pandas())  # Only convert for display
            
            # Check if we have the required columns for plotting
            required_columns = ['hotel_name', 'check_in_date', 'hotel_price']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                st.error(f"Missing required columns for plotting: {missing_columns}")
                st.info("Available columns: " + ", ".join(df.columns))
                return
            
            # Main price chart - Plotly works directly with Polars!
            st.subheader("📈 Динамика цен по отелям")
            
            # Chart type selection
            chart_type = st.radio(
                "Выберите тип графика:",
                ["Линейный график", "График с маркерами", "Только маркеры"],
                horizontal=True,
                key=f"chart_type_{group.group_name}"
            )
            
            # Create the plot with proper mode control
            if chart_type == "Линейный график":
                mode = 'lines'
            elif chart_type == "График с маркерами":
                mode = 'lines+markers'
            else:
                mode = 'markers'
            
            # Use go.Figure() with go.Scatter() for full mode control
            fig = go.Figure()
            
            # Get unique hotels and add traces
            hotels = date_filtered_df['hotel_name'].unique().to_list()
            
            for hotel in hotels:
                hotel_data = date_filtered_df.filter(pl.col('hotel_name') == hotel)
                
                fig.add_trace(go.Scatter(
                    x=hotel_data['check_in_date'].to_list(),
                    y=hotel_data['hotel_price'].to_list(),
                    mode=mode,
                    name=hotel,
                    line=dict(width=2),
                    marker=dict(size=6),
                    customdata=hotel_data['day_of_week'].to_list(),  # Add day of week data
                    hovertemplate='<b>%{fullData.name}</b><br>' +
                                'Дата: %{x}<br>' +
                                'День недели: %{customdata}<br>' +
                                'Цена: ₽%{y:,.0f}<br>' +
                                '<extra></extra>'
                ))
            
            # Add date range slider
            fig.update_xaxes(rangeslider_visible=True)
            
            # Set the title
            fig.update_layout(title=f'Динамика цен отелей - {group.group_name}')
            
            # Update layout for better appearance
            fig.update_layout(
                width=None,  # Let streamlit control width
                height=600,
                hovermode='x unified',
                legend=dict(
                    orientation="v",
                    yanchor="top",
                    y=1,
                    xanchor="left",
                    x=1.02
                ),
                xaxis_title='Дата заезда',
                yaxis_title='Цена (₽)',
                template='plotly_white'
            )
            
            # Update x-axis to rotate labels and show grid
            fig.update_xaxes(tickangle=45, showgrid=True, gridwidth=1, gridcolor='LightGray')
            fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='LightGray')
            
            # Display the plot in Streamlit
            st.plotly_chart(fig, use_container_width=True)
            
            
            # Price comparison table - transposed format
            st.subheader("🗓️ Таблица цен по датам")

            # Create pivot table with dates as columns and hotels as rows
            try:
                # Get unique dates and hotels
                unique_dates = sorted(date_filtered_df['check_in_date'].unique().to_list())
                unique_hotels = date_filtered_df['hotel_name'].unique().to_list()
                
                # Create a pivot-like structure using Polars
                pivot_data = {}
                
                # Initialize the data structure
                for hotel in unique_hotels:
                    pivot_data[hotel] = {}
                    for date in unique_dates:
                        # Get price for this hotel and date combination
                        price_data = date_filtered_df.filter(
                            (pl.col('hotel_name') == hotel) & 
                            (pl.col('check_in_date') == date)
                        )
                        
                        if not price_data.is_empty():
                            # If multiple prices for same hotel/date, take the average
                            avg_price = price_data['hotel_price'].mean()
                            pivot_data[hotel][date] = f"₽{avg_price:,.0f}"
                        else:
                            pivot_data[hotel][date] = "-"
                
                # Convert to pandas DataFrame for easier table display
                
                
                # Create DataFrame from pivot_data
                table_df = pd.DataFrame(pivot_data).T  # Transpose so hotels are rows
                
                # Format column names (dates) for better display
                table_df.columns = [date.strftime('%d.%m.%Y') for date in table_df.columns]
                
                # Calculate row averages (average price per hotel)
                avg_prices = []
                for hotel in unique_hotels:
                    hotel_prices = date_filtered_df.filter(pl.col('hotel_name') == hotel)['hotel_price']
                    if not hotel_prices.is_empty():
                        avg_price = hotel_prices.mean()
                        avg_prices.append(f"₽{avg_price:,.0f}")
                    else:
                        avg_prices.append("-")
                
                table_df['Средняя цена'] = avg_prices
                
                # Calculate daily averages (bottom row)
                daily_averages = {}
                for date in unique_dates:
                    daily_data = date_filtered_df.filter(pl.col('check_in_date') == date)
                    if not daily_data.is_empty():
                        daily_avg = daily_data['hotel_price'].mean()
                        daily_averages[date.strftime('%d.%m.%Y')] = f"₽{daily_avg:,.0f}"
                    else:
                        daily_averages[date.strftime('%d.%m.%Y')] = "-"
                
                # Add overall average
                overall_avg = date_filtered_df['hotel_price'].mean()
                daily_averages['Средняя цена'] = f"₽{overall_avg:,.0f}"
                
                # Add daily averages as a new row
                daily_avg_series = pd.Series(daily_averages, name='Среднее по дням')
                table_df = pd.concat([table_df, daily_avg_series.to_frame().T])
                
                # Display the table
                st.dataframe(
                    table_df,
                    use_container_width=True,
                    height=min(400, (len(table_df) + 1) * 35)  # Dynamic height based on rows
                )
                
                # Add download button for the table
                csv_data = table_df.to_csv(index=True, encoding='utf-8-sig')
                st.download_button(
                    label="📥 Скачать таблицу как CSV",
                    data=csv_data,
                    file_name=f"price_table_{group.group_name}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv"
                )
                
            except Exception as e:
                st.error(f"Ошибка при создании таблицы цен: {str(e)}")
                st.info("Убедитесь, что данные содержат корректные цены и даты.")
            
            # Additional analytics using Polars
            st.subheader("📊 Дополнительная аналитика")
            
            # Price comparison table using Polars
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Статистика по отелям")
                
                # Calculate hotel statistics using Polars
                hotel_stats = date_filtered_df.group_by('hotel_name').agg([
                    pl.col('hotel_price').min().alias('Минимальная цена'),
                    pl.col('hotel_price').max().alias('Максимальная цена'),
                    pl.col('hotel_price').mean().alias('Средняя цена'),
                    pl.col('hotel_price').count().alias('Количество записей')
                ]).sort('hotel_name')
                
                # Convert to pandas only for display formatting
                hotel_stats_pandas = hotel_stats.to_pandas()
                
                # Format the prices with ruble symbol
                for col in ['Минимальная цена', 'Максимальная цена', 'Средняя цена']:
                    hotel_stats_pandas[col] = hotel_stats_pandas[col].apply(lambda x: f"₽{x:,.0f}")
                
                st.dataframe(hotel_stats_pandas.set_index('hotel_name'))
            
            with col2:
                st.subheader("Ценовые диапазоны")
                # Create a box plot - works directly with Polars
                fig_box = px.box(
                    date_filtered_df,
                    x='hotel_name',
                    y='hotel_price',
                    title='Распределение цен по отелям',
                    labels={
                        'hotel_name': 'Отель',
                        'hotel_price': 'Цена (₽)'
                    }
                )
                
                fig_box.update_layout(
                    height=400,
                    xaxis_tickangle=15,
                    template='plotly_white'
                )
                
                st.plotly_chart(fig_box, use_container_width=True)
                
            
            # Date range analysis
            if date_filtered_df['check_in_date'].n_unique() > 1:
                st.subheader("📅 Анализ по датам")
                
                # Average price by date using Polars
                daily_avg = date_filtered_df.group_by('check_in_date').agg([
                    pl.col('hotel_price').mean().alias('hotel_price'),
                    pl.col('day_of_week').first().alias('day_of_week')  # Get day of week
                ]).sort('check_in_date')
                
                # Create enhanced daily price plot with day of week information
                fig_daily = go.Figure()
                
                fig_daily.add_trace(go.Scatter(
                    x=daily_avg['check_in_date'].to_list(),
                    y=daily_avg['hotel_price'].to_list(),
                    mode='lines+markers',
                    name='Средняя цена',
                    line=dict(width=3, color='#1f77b4'),
                    marker=dict(size=8),
                    customdata=daily_avg['day_of_week'].to_list(),
                    hovertemplate='<b>Средняя цена</b><br>' +
                                'Дата: %{x}<br>' +
                                'День недели: %{customdata}<br>' +
                                'Средняя цена: ₽%{y:,.0f}<br>' +
                                '<extra></extra>'
                ))
                
                fig_daily.update_layout(
                    title='Средняя цена по датам',
                    xaxis_title='Дата заезда',
                    yaxis_title='Средняя цена (₽)',
                    height=400,
                    template='plotly_white'
                )
                fig_daily.update_xaxes(rangeslider_visible=True)
                st.plotly_chart(fig_daily, use_container_width=True)
            
            # Day of week analysis - bonus feature!
            st.subheader("📅 Анализ по дням недели")
            
            # Average price by day of week
            dow_avg = date_filtered_df.group_by('day_of_week').agg([
                pl.col('hotel_price').mean().alias('Средняя цена'),
                pl.col('hotel_price').count().alias('Количество записей')
            ])
            
            # Sort by day of week order (Monday = 0, Sunday = 6)
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            dow_avg_sorted = []
            for day in day_order:
                day_data = dow_avg.filter(pl.col('day_of_week') == day)
                if not day_data.is_empty():
                    dow_avg_sorted.append(day_data)
            
            if dow_avg_sorted:
                dow_avg_final = pl.concat(dow_avg_sorted)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Bar chart for average prices by day
                    fig_dow = px.bar(
                        dow_avg_final,
                        x='day_of_week',
                        y='Средняя цена',
                        title='Средняя цена по дням недели',
                        labels={
                            'day_of_week': 'День недели',
                            'Средняя цена': 'Средняя цена (₽)'
                        },
                        color='Средняя цена',
                        color_continuous_scale='viridis'
                    )
                    
                    fig_dow.update_layout(
                        height=400,
                        template='plotly_white',
                        xaxis_tickangle=45
                    )
                    
                    st.plotly_chart(fig_dow, use_container_width=True)
                
                with col2:
                    # Display statistics table
                    st.subheader("Статистика по дням")
                    dow_display = dow_avg_final.to_pandas()
                    dow_display['Средняя цена'] = dow_display['Средняя цена'].apply(lambda x: f"₽{x:,.0f}")
                    dow_display = dow_display.rename(columns={'day_of_week': 'День недели'})
                    st.dataframe(dow_display.set_index('День недели'))
        
    except Exception as e:
        st.error(f"Ошибка загрузки данных для группы '{group.group_name}': {str(e)}")
        st.info("Убедитесь, что у вас есть собранные данные о ценах для этой группы.")

# Main app
st.title("Hotel Price Monitor")

# Get all hotel groups
groups = load_hotel_groups()

# If no groups exist yet, show a welcome screen
if not groups:
    st.info("🏨 Добро пожаловать в Hotel Price Monitor!")
    st.markdown("""
    Для начала работы создайте первую группу отелей:
    """)
    
    # Add new group section
    with st.expander("➕ Создать первую группу отелей", expanded=True):
        with st.form("new_group_form"):
            new_group_name = st.text_input("Название группы", placeholder="Например: Сочи центр")
            description = st.text_area("Описание", placeholder="Необязательное описание группы")
            location = st.text_input("Локация", placeholder="Необязательная локация отелей в этой группе")
            submitted = st.form_submit_button("Создать группу")
            if submitted and new_group_name:
                if add_new_group(new_group_name, description, location):
                    st.success(f"Группа создана: {new_group_name}")
                    st.rerun()
                else:
                    st.error(f"Группа с названием '{new_group_name}' уже существует")
else:
    # Create top-level tabs for each hotel group
    group_names = [group.group_name for group in groups]
    
    # Add a special tab for adding new groups
    all_tab_names = group_names + ["➕ Добавить группу"]
    top_level_tabs = st.tabs(all_tab_names)
    
    # Render tabs for existing groups
    for i, group in enumerate(groups):
        with top_level_tabs[i]:
            st.header(f"Группа: {group.group_name}")
            
            # Create sub-tabs for each group
            tab1, tab2, tab3 = st.tabs(["Управление отелями", "Диапазоны дат (Сбор данных)", "Анализ цен"])
            
            # Tab 1: Manage Links for this group
            with tab1:
                render_manage_links_tab(group)
            
            # Tab 2: Price Ranges for this group
            with tab2:
                render_price_ranges_tab(group.group_name)
            
            # Tab 3: Price Analytics for this group
            with tab3:
                render_price_analytics_tab(group)
    
    # Last tab for adding new groups
    with top_level_tabs[-1]:
        st.header("➕ Добавить новую группу отелей")
        
        with st.form("new_group_form_main"):
            new_group_name = st.text_input("Название группы")
            description = st.text_area("Описание", placeholder="Необязательное описание группы")
            location = st.text_input("Локация", placeholder="Необязательная локация отелей в этой группе")
            submitted = st.form_submit_button("Добавить группу")
            if submitted and new_group_name:
                if add_new_group(new_group_name, description, location):
                    st.success(f"Группа добавлена: {new_group_name}")
                    st.rerun()
                else:
                    st.error(f"Группа с названием '{new_group_name}' уже существует")
        
        # Show existing groups summary
        if groups:
            st.subheader("📋 Обзор существующих групп")
            
            summary_data = []
            for group in groups:
                hotel_count = len(group.hotels)
                price_ranges_count = len(redis_storage.get_price_ranges(group.group_name))
                
                summary_data.append({
                    "Группа": group.group_name,
                    "Описание": group.description or "-",
                    "Локация": group.location or "-",
                    "Количество отелей": hotel_count,
                    "Диапазонов дат": price_ranges_count
                })
            
            summary_df = pd.DataFrame(summary_data)
            st.dataframe(summary_df, hide_index=True, use_container_width=True)

st.markdown("---")
st.caption("Hotel Price Monitor App - Prototype Version")