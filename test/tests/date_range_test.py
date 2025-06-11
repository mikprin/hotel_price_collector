from datetime import datetime, timedelta

from hotel_price_absorber_src.tasks import generate_date_pairs


def test_generate_date_pairs():
    
    start_date_raw = "01.01.2023"
    end_date_raw = "10.01.2023"
    days_of_stay = 2
    
    start_date = datetime.strptime(start_date_raw, "%d.%m.%Y")
    end_date = datetime.strptime(end_date_raw, "%d.%m.%Y")
    # Generate date pairs for tend_datehe given range
    date_pairs = generate_date_pairs(start_date, end_date, days_of_stay)

    print(f"Generated date pairs: {date_pairs}")
    assert len(date_pairs) == 8, "Should generate 9 date pairs for the given range and stay length"
    assert date_pairs[0] == (datetime(2023, 1, 1), datetime(2023, 1, 3)), "First date pair should be (01.01.2023, 03.01.2023)"
    assert date_pairs[-1] == (datetime(2023, 1, 9), datetime(2023, 1, 11)), "Last date pair should be (09.01.2023, 11.01.2023)"