import pandas as pd
import sqlite3


def main():
    def get_table_df(table_name: str) -> pd.DataFrame:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, sqliteConnection)
        return df

    with sqlite3.connect('S30 ETL Assignment.db') as sqliteConnection:
        sales_df = get_table_df("sales")
        customers_df = get_table_df("customers")
        orders_df = get_table_df("orders")
        items_df = get_table_df("items")

    merged_df = orders_df.merge(items_df, how="left")
    merged_df = merged_df.merge(sales_df, how="left")
    merged_df = merged_df.merge(customers_df, how="left")

    mask = merged_df["quantity"] > 0
    mask &= merged_df["age"].isin(range(18, 36))
    df = merged_df[mask].groupby(["customer_id", "age", "item_name"])["quantity"].sum().reset_index()
    df["quantity"] = df["quantity"].astype(int)

    column_names = {
        "customer_id": "Customer",
        "age": "Age",
        "item_name": "Item",
        "quantity": "Quantity"
    }
    df = df.rename(columns=column_names)
    df.to_csv("results/pandas_solution.csv", sep=";", index=False)

    return

if __name__ == "__main__":
    main()
