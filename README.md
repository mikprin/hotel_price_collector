# Hotel Price Absorber

A tool for collecting and analyzing hotel prices from Ostrovok.ru across multiple date ranges.

## Overview

Hotel Price Absorber helps you track and compare hotel prices over specific date ranges. The tool scrapes pricing data from Ostrovok.ru for a list of hotels and saves the results to CSV for analysis.

## Features

- Process raw hotel URLs from Ostrovok.ru
- Collect prices across multiple date ranges
- Support for date ranges in Russian format
- Flexible stay length configuration
- Anti-scraping measures with randomized delays
- Comprehensive data output with price statistics

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/hotel-price-absorber.git
cd hotel-price-absorber

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Collecting Prices by Date Range

```bash
python price_collector.py --links hotel_links.txt --dates date_ranges.txt --stay 2 --output results/hotel_prices.csv
```

### Parameters

- `--links`: File containing raw hotel URLs (one per line)
- `--dates`: File containing date ranges in Russian format (one per line)
- `--stay`: Length of stay in days (default: 1)
- `--output`: Path for the output CSV file

### Input Examples

**hotel_links.txt**:
```
https://ostrovok.ru/hotel/russia/st._petersburg/mid10006831/maximusvertical_aparthotel/?dates=08.11.2024-09.11.2024&guests=2
https://ostrovok.ru/hotel/russia/st._petersburg/mid10675845/simple_stunningaparts_apartments/?dates=03.10.2024-05.10.2024&guests=2
```

**date_ranges.txt**:
```
Даты с 20 по 30 мая
С 1 по 10 июня
с 3 по 10 июля
```

## Output

The script generates a CSV file containing:
- Hotel details (name, URL, location)
- Price information (amount, currency)
- Date information (check-in, check-out, date range)
- Additional metadata from the original source

## Requirements

- Python 3.8+
- polars
- Requires the `hotel_price_absorber_src` package

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.