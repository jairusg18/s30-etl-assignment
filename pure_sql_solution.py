import csv
import sqlite3


def main():
    query = """
        SELECT c.customer_id, c.age, i.item_name, SUM(o.quantity) AS item_sum
        FROM orders o
        LEFT JOIN items i ON o.item_id = i.item_id
        LEFT JOIN sales s ON o.sales_id = s.sales_id
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        WHERE c.age BETWEEN 18 AND 35
        GROUP BY c.customer_id, i.item_name
        HAVING item_sum > 0
        ORDER BY c.customer_id, i.item_name;
    """

    with sqlite3.connect("S30 ETL Assignment.db") as sqliteConnection:
        cursor = sqliteConnection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()

    with open("results/pure_sql_solution.csv", "w", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["Customer", "Age", "Item", "Quantity"])
        writer.writerows(results)
    
    return

if __name__ == "__main__":
    main()
