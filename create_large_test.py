#!/usr/bin/env python3
"""Create large test CSV file with 10,000+ rows"""

import csv
import random
from datetime import datetime, timedelta

def generate_large_csv(filename, num_rows=12000):
    """Generate a large CSV file for performance testing"""
    
    categories = ["Electronics", "Books", "Clothing", "Sports", "Home & Garden", "Tools", "Automotive", "Beauty"]
    statuses = ["Active", "Inactive", "Pending", "Discontinued"]
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header
        writer.writerow(['id', 'name', 'description', 'category', 'price', 'status', 'date_created', 'sku', 'supplier'])
        
        # Generate data rows
        base_date = datetime(2024, 1, 1)
        
        for i in range(1, num_rows + 1):
            # Generate random data
            product_name = f"Product {i:05d}"
            description = f"Description for product {i} - {random.choice(['Premium', 'Standard', 'Basic', 'Deluxe'])} quality item"
            category = random.choice(categories)
            price = round(random.uniform(9.99, 999.99), 2)
            status = random.choice(statuses)
            date_created = (base_date + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
            sku = f"SKU-{i:06d}-{category[:3].upper()}"
            supplier = f"Supplier_{random.randint(1, 50):03d}"
            
            writer.writerow([i, product_name, description, category, price, status, date_created, sku, supplier])
    
    print(f"✅ Created {filename} with {num_rows} rows")

if __name__ == '__main__':
    generate_large_csv('large_test.csv', 12000)
    
    # Also create medium test file
    generate_large_csv('medium_test.csv', 3000)
    
    # Show file sizes
    import os
    large_size = os.path.getsize('large_test.csv')
    medium_size = os.path.getsize('medium_test.csv')
    
    print(f"📊 large_test.csv: {large_size:,} bytes ({large_size/1024/1024:.1f} MB)")
    print(f"📊 medium_test.csv: {medium_size:,} bytes ({medium_size/1024/1024:.1f} MB)")