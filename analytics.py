pip install pymysql pandas


sudo apt update
sudo apt install python3-pip
pip3 install pymysql pandas


import pymysql
import pandas as pd

# Параметры подключения
host = '95.131.149.21'
port = 3306
user = 'mgpu_ico_etl_16'
password = 'Za6^4xX#*l'
database = 'mgpu_ico_etl_16'

def run_query(query, description):
    """Выполнить запрос и вывести результаты в консоль и CSV"""
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset='utf8mb4'
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=col_names)
            print(f"\n=== {description} ===\n")
            print(df.to_string(index=False))
            filename = f"{description.replace(' ', '_').lower()}.csv"
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"\n✅ Результаты сохранены в файл: {filename}")
    finally:
        conn.close()

# Анализ категорий
query1 = """
SELECT 
    p.category,
    COUNT(DISTINCT o.order_id) AS number_of_orders,
    SUM(o.quantity) AS total_quantity,
    SUM(o.sales) AS total_sales,
    AVG(o.sales) AS avg_sales_per_order,
    SUM(o.profit) AS total_profit,
    AVG(o.profit) AS avg_profit
FROM orders o
JOIN products p ON o.product_id = p.product_id
GROUP BY p.category
ORDER BY total_sales DESC;
"""
run_query(query1, "Анализ категорий")

# Отчет по сегментам
query2 = """
SELECT 
    c.segment,
    COUNT(DISTINCT o.order_id) AS number_of_orders,
    SUM(o.quantity) AS total_quantity,
    SUM(o.sales) AS total_sales,
    AVG(o.sales) AS avg_sales_per_order,
    SUM(o.profit) AS total_profit,
    AVG(o.profit) AS avg_profit
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.segment
ORDER BY total_sales DESC;
"""
run_query(query2, "Отчет по сегментам")

print("\n🎉 Анализ завершён. CSV-файлы созданы.")
