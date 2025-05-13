import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import random
from datetime import datetime, timedelta

# Set page config
st.set_page_config(
    page_title="Price Monitor",
    page_icon="📊",
    layout="wide"
)

# Initialize session state for links
if 'links' not in st.session_state:
    st.session_state.links = [
        {"name": "Example Product 1", "url": "https://example.com/product1", "last_price": 99.99},
        {"name": "Example Product 2", "url": "https://example.com/product2", "last_price": 149.99},
        {"name": "Example Product 3", "url": "https://example.com/product3", "last_price": 79.99},
    ]

# Function to load links (placeholder)
def load_links():
    # In a real app, this would load from a database
    return st.session_state.links

# Function to save links (placeholder)
def save_links(links):
    # In a real app, this would save to a database
    st.session_state.links = links
    return True

# Function to generate mock price history data
def generate_price_data(product_name, days=30):
    dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days)]
    
    # Find the product's current price
    current_price = 100.0  # Default
    for link in st.session_state.links:
        if link["name"] == product_name:
            current_price = link["last_price"]
            break
    
    # Generate price fluctuations
    prices = [current_price]
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
st.title("Price Monitor")

# Create tabs
tab1, tab2 = st.tabs(["Manage Links", "Price Analytics"])

# Tab 1: Manage Links
with tab1:
    st.header("Your Tracked Products")
    
    links = load_links()
    
    # Display existing links in a table
    if links:
        df = pd.DataFrame(links)
        edited_df = st.data_editor(
            df,
            column_config={
                "name": "Product Name",
                "url": "Product URL",
                "last_price": st.column_config.NumberColumn(
                    "Last Price ($)",
                    format="%.2f",
                ),
            },
            hide_index=True,
            num_rows="dynamic"
        )
        
        # Save button
        if st.button("Save Changes"):
            save_links(edited_df.to_dict('records'))
            st.success("Links saved successfully!")
    else:
        st.info("No links added yet. Add your first product link below.")
    
    # Add new link form
    with st.expander("Add New Product"):
        with st.form("new_link_form"):
            col1, col2 = st.columns(2)
            with col1:
                new_name = st.text_input("Product Name")
            with col2:
                new_url = st.text_input("Product URL")
            new_price = st.number_input("Current Price ($)", min_value=0.0, format="%.2f")
            
            submitted = st.form_submit_button("Add Product")
            if submitted and new_name and new_url:
                links.append({
                    "name": new_name,
                    "url": new_url,
                    "last_price": new_price
                })
                save_links(links)
                st.success(f"Added {new_name} to your tracked products!")
                st.experimental_rerun()

# Tab 2: Price Analytics
with tab2:
    st.header("Price History")
    
    links = load_links()
    
    if not links:
        st.info("No products added yet. Please add products in the 'Manage Links' tab.")
    else:
        # Product selector
        product_names = [link["name"] for link in links]
        selected_product = st.selectbox("Select Product", product_names)
        
        # Date range selector
        date_range = st.slider("Date Range (days)", 7, 90, 30)
        
        # Get price data for selected product
        price_data = generate_price_data(selected_product, days=date_range)
        
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
        ax.set_title(f"{selected_product} - Price History")
        ax.set_xlabel("Date")
        ax.set_ylabel("Price ($)")
        ax.grid(True)
        
        # Show every nth label to avoid crowding
        n = max(1, len(price_data) // 10)
        plt.xticks(range(0, len(price_data), n), price_data['date'][::n], rotation=45)
        
        plt.tight_layout()
        st.pyplot(fig)
        
        # Show raw data
        with st.expander("View Raw Data"):
            st.dataframe(price_data)
        
        # Add download button for CSV
        st.download_button(
            label="Download Price Data as CSV",
            data=price_data.to_csv(index=False).encode('utf-8'),
            file_name=f"{selected_product}_price_history.csv",
            mime="text/csv",
        )

# Footer
st.markdown("---")
st.caption("Price Monitor App - Prototype Version")