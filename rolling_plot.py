from hotel_price_absorber_src.ostrovok.scraper import get_price_from_simple_url
import os
from datetime import datetime, timedelta
import polars as pl
import time
import random
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def create_date_specific_url(url, start_date, length_of_stay=1):
    """
    Replace the $DATES placeholder in the URL with specific dates.
    Format: 15.04.2025-17.04.2025
    
    Args:
        url: URL with $DATES placeholder
        start_date: datetime object for check-in date
        length_of_stay: number of nights to stay
    
    Returns:
        Updated URL with specific dates
    """
    # Calculate checkout date
    end_date = start_date + timedelta(days=length_of_stay)
    
    # Format the dates in the required format
    formatted_dates = f"{start_date.strftime('%d.%m.%Y')}-{end_date.strftime('%d.%m.%Y')}"
    
    # Replace the placeholder in the URL
    updated_url = url.replace("$DATES", formatted_dates)
    
    return updated_url

def add_date_placeholder(url):
    """
    Add $DATES placeholder to the URL if not present.
    """
    if "$DATES" not in url:
        if url.endswith("/"):
            url += "?dates=$DATES&guests=2"
        else:
            url += "&dates=$DATES&guests=2"
    return url

def collect_daily_hotel_prices(links, start_date, num_days=30, length_of_stay=1):
    """
    Collect daily prices for hotels over a range of dates.
    
    Args:
        links: List of hotel URLs with $DATES placeholder
        start_date: datetime object for the first date to check
        num_days: Number of days to collect prices for
        length_of_stay: Length of stay for each price check
    
    Returns:
        Dictionary with hotel names as keys and lists of (date, price) tuples as values
    """
    # Initialize data structure to store results
    hotel_prices = {}
    
    print(f"Starting price collection for {len(links)} hotels over {num_days} days...")
    
    # For each day in the range
    for day_offset in range(num_days):
        check_date = start_date + timedelta(days=day_offset)
        print(f"\nCollecting prices for {check_date.strftime('%d.%m.%Y')}:")
        
        # For each hotel
        for i, link in enumerate(links):
            try:
                print(f"  Processing hotel {i+1}/{len(links)}")
                
                # Create URL with specific date
                updated_link = create_date_specific_url(link, check_date, length_of_stay)
                
                # Get price data
                price_data = get_price_from_simple_url(updated_link)
                hotel_name = price_data.hotel_name
                price = price_data.hotel_price
                
                # Store the data
                if hotel_name not in hotel_prices:
                    hotel_prices[hotel_name] = []
                
                hotel_prices[hotel_name].append((check_date, price))
                
                print(f"    {hotel_name}: {price} {price_data.hotel_currency}")
                
                # Add a small random delay to avoid triggering anti-scraping measures
                time.sleep(random.uniform(1, 3))
                
            except Exception as e:
                print(f"    Error processing {link} for {check_date.strftime('%d.%m.%Y')}: {e}")
    
    return hotel_prices

def save_to_csv(hotel_prices, filename="daily_hotel_prices.csv"):
    """
    Convert collected price data to a Polars DataFrame and save to CSV.
    
    Args:
        hotel_prices: Dictionary with hotel names as keys and lists of (date, price) tuples as values
        filename: Path to save the CSV file
    
    Returns:
        Polars DataFrame with the data
    """
    if not hotel_prices:
        print("No prices to save.")
        return None
    
    # Prepare data for DataFrame
    data = []
    for hotel_name, prices in hotel_prices.items():
        for date, price in prices:
            data.append({
                "hotel_name": hotel_name,
                "date": date.strftime('%Y-%m-%d'),
                "price": price
            })
    
    # Create DataFrame
    df = pl.DataFrame(data)
    
    # Save DataFrame to CSV file
    try:
        df.write_csv(filename)
        print(f"Prices saved to {filename}")
    except Exception as e:
        print(f"Error saving to CSV: {e}")
    
    return df

def plot_hotel_prices(hotel_prices, save_path=None):
    """
    Create a line plot of hotel prices over time.
    
    Args:
        hotel_prices: Dictionary with hotel names as keys and lists of (date, price) tuples as values
        save_path: Path to save the plot image (optional)
    """
    plt.figure(figsize=(12, 6))
    
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    markers = ['o', 's', '^', 'D', 'v']
    
    for i, (hotel_name, prices) in enumerate(hotel_prices.items()):
        # Sort prices by date
        prices.sort(key=lambda x: x[0])
        
        # Extract dates and prices
        dates = [p[0] for p in prices]
        price_values = [p[1] for p in prices]
        
        # Plot the data
        color_idx = i % len(colors)
        marker_idx = i % len(markers)
        plt.plot(dates, price_values, marker=markers[marker_idx], linestyle='-', 
                 color=colors[color_idx], label=hotel_name, markersize=6)
    
    # Format the plot
    plt.title('Daily Hotel Prices Comparison', fontsize=16)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Price', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend(loc='best')
    
    # Format x-axis dates
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%b'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=5))
    plt.gcf().autofmt_xdate()
    
    # Add price labels to the last point of each line
    for hotel_name, prices in hotel_prices.items():
        if prices:
            last_date = prices[-1][0]
            last_price = prices[-1][1]
            plt.annotate(f"{last_price}", 
                         (last_date, last_price),
                         textcoords="offset points",
                         xytext=(5, 5),
                         ha='left')
    
    plt.tight_layout()
    
    # Save the plot if a path is provided
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved to {save_path}")
    
    plt.show()

def main():
    # Hotel links
    links = [
        "https://ostrovok.ru/hotel/russia/st._petersburg/mid10006831/maximusvertical_aparthotel/?dates=$DATES&guests=2",
        "https://ostrovok.ru/hotel/russia/st._petersburg/mid10675845/simple_stunningaparts_apartments/?dates=$DATES&guests=2",
        "https://ostrovok.ru/hotel/russia/st._petersburg/mid9721358/wei_by_vertical_hotel/?dates=$DATES&guests=2",
    ]
    
    # Create directory for output files
    output_dir = "hotel_prices_analysis"
    os.makedirs(output_dir, exist_ok=True)
    
    # Set start date and collection parameters
    start_date = datetime.today() + timedelta(days=7)  # Start a week from today
    num_days = 30  # Collect prices for 30 days
    length_of_stay = 2  # 2-night stay for each check
    
    print(f"Collecting daily prices for {num_days} days starting from {start_date.strftime('%d.%m.%Y')}")
    print(f"Each check is for a {length_of_stay}-night stay\n")
    
    # Collect daily prices
    hotel_prices = collect_daily_hotel_prices(links, start_date, num_days, length_of_stay)
    
    # Save data to CSV
    csv_path = os.path.join(output_dir, "daily_hotel_prices.csv")
    df = save_to_csv(hotel_prices, csv_path)
    
    # Create and save the plot
    plot_path = os.path.join(output_dir, "daily_hotel_prices_plot.png")
    plot_hotel_prices(hotel_prices, plot_path)
    
    print("\nAnalysis complete!")
    
    if df is not None:
        print("\nSample of collected data:")
        print(df.head(10))

if __name__ == "__main__":
    main()