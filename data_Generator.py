import random
import faker
from datetime import datetime, timedelta
import csv

# Initialize Faker for generating fake customer data
fake = faker.Faker()

# Sample data for products
products = [
    {'product_id': 1, 'product_name': 'Laptop', 'product_category': 'Electronics', 'product_price': 799.99},
    {'product_id': 2, 'product_name': 'Smartphone', 'product_category': 'Electronics', 'product_price': 499.99},
    {'product_id': 3, 'product_name': 'T-shirt', 'product_category': 'Apparel', 'product_price': 19.99},
    {'product_id': 4, 'product_name': 'Headphones', 'product_category': 'Electronics', 'product_price': 59.99},
]

# Function to generate a random order
def generate_order(order_id):
    customer = fake.name()
    email = fake.email()
    phone = fake.phone_number()
    order_date = fake.date_this_year()
    
    # Select random product
    product = random.choice(products)
    product_id = product['product_id']
    product_name = product['product_name']
    product_category = product['product_category']
    product_price = product['product_price']
    
    # Generate random order details
    quantity = random.randint(1, 5)
    discount = random.uniform(0, 0.3)  # 0-30% discount
    total_price = product_price * quantity * (1 - discount)
    
    # Generate shipping details
    shipping_address = fake.address()
    shipping_city = fake.city()
    shipping_state = fake.state()
    shipping_zip = fake.zipcode()
    
    # Payment and order status
    payment_status = random.choice(['Paid', 'Pending', 'Failed'])
    order_status = random.choice(['Shipped', 'Pending', 'Cancelled'])
    
    # Generate sales tax (5-10% of total price)
    sales_tax = total_price * random.uniform(0.05, 0.1)
    
    # Generate promo code or leave empty
    promo_code = fake.word() if random.random() > 0.7 else ''
    
    # Create the order record
    return {
        'order_id': order_id,
        'customer_id': order_id,  # Using order_id as a placeholder for customer_id
        'customer_name': customer,
        'customer_email': email,
        'customer_phone': phone,
        'product_id': product_id,
        'product_name': product_name,
        'product_category': product_category,
        'product_price': product_price,
        'quantity': quantity,
        'order_date': order_date,
        'shipping_address': shipping_address,
        'shipping_city': shipping_city,
        'shipping_state': shipping_state,
        'shipping_zip': shipping_zip,
        'total_price': total_price,
        'payment_status': payment_status,
        'order_status': order_status,
        'discount': discount,
        'sales_tax': sales_tax,
        'promo_code': promo_code
    }

# Generate data for 10 orders
orders = [generate_order(i) for i in range(1, 1001)]

csv_file_path = 'SharePoint_CSV/orders.csv'

# Define the CSV headers (columns)
headers = [
    'order_id', 'customer_id', 'customer_name', 'customer_email', 'customer_phone',
    'product_id', 'product_name', 'product_category', 'product_price', 'quantity', 'order_date',
    'shipping_address', 'shipping_city', 'shipping_state', 'shipping_zip', 'total_price',
    'payment_status', 'order_status', 'discount', 'sales_tax', 'promo_code'
]

# Write orders to the CSV file
with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=headers)
    writer.writeheader()  # Write the header row
    for order in orders:
        writer.writerow(order)  # Write each order

print(f"Orders have been saved to {csv_file_path}")