python3 << EOF
import pandas as pd
import numpy as np

orders = pd.read_csv('Orders.csv')
unique_products = orders[['Product ID', 'Product Name', 'Category', 'Sub-Category']].drop_duplicates()
orders['Unit Price'] = orders['Sales'] / orders['Quantity']
avg_price = orders.groupby('Product ID')['Unit Price'].mean().reset_index()
products = pd.merge(unique_products, avg_price, on='Product ID')
products['Stock Quantity'] = np.random.randint(10, 101, size=len(products))
products.columns = ['product_id', 'product_name', 'category', 'sub_category', 'price', 'stock_quantity']
products.to_csv('products.csv', index=False, header=False)
print("Файл products.csv создан, строк:", len(products))
EOF
