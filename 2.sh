python3 -c "
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

products = pd.read_csv('products.csv', header=None, names=['product_id','product_name','category','sub_category','price','stock_quantity'])
reviews = []
review_id = 1
for _, row in products.iterrows():
    num_reviews = random.randint(0, 5)
    for _ in range(num_reviews):
        rating = random.randint(1, 5)
        comment = random.choice(['Отличный товар','Хороший','Нормально','Плохой','Ужасный','Рекомендую','Не понравился'])
        days_ago = random.randint(0, 60)
        review_date = (datetime.now() - timedelta(days=days_ago)).date()
        reviews.append([review_id, row['product_id'], rating, comment, review_date])
        review_id += 1
df_reviews = pd.DataFrame(reviews, columns=['review_id','product_id','rating','comment','review_date'])
df_reviews.to_csv('reviews.csv', index=False)
print('reviews.csv created with', len(df_reviews), 'reviews')
"






python3 -c "
import pandas as pd
import numpy as np

returns = pd.read_csv('Returns.csv', encoding='cp1252')
orders = pd.read_csv('Orders.csv', encoding='cp1252')
merged = pd.merge(returns, orders, on='Order ID', how='inner')
result = merged[['Order ID', 'Product ID', 'Order Date', 'Quantity']].copy()
result['Return Date'] = pd.to_datetime(result['Order Date']) + pd.to_timedelta(np.random.randint(1, 30, size=len(result)), unit='D')
reasons = ['брак','не подошел','царапина','не работает','передумал','доставка поздно']
result['Reason'] = np.random.choice(reasons, size=len(result))
result['Return ID'] = range(1, len(result)+1)
result.rename(columns={'Product ID':'product_id','Return Date':'return_date','Reason':'reason','Return ID':'return_id','Quantity':'quantity'}, inplace=True)
result[['return_id','product_id','return_date','reason','quantity']].to_excel('returns.xlsx', index=False)
print('returns.xlsx created with', len(result), 'returns')
"


