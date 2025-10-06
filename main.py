from fastmcp import FastMCP
import os
import aiosqlite
import tempfile
import json

# Use a temporary directory which should be writable
TEMP_DIR = tempfile.gettempdir()
DB_PATH =  "sales.db"
print(f"Database path: {DB_PATH}")

mcp = FastMCP("Data Companion")

# ------------------ Tool: Execute SQL ------------------

@mcp.tool()
async def execute_sql(query: str):
    """Execute a raw SQL query on the database without any checks."""
    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute(query)
        if cur.description:  # means SELECT or similar
            rows = await cur.fetchall()
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in rows]
        else:
            await conn.commit()
            return {"message": "Query executed successfully."}

# ------------------ Resources ------------------

@mcp.resource("sql:///prompt", mime_type="text/plain")
def sql_prompt():
    """Provide a prompt describing the DB schema and SQL generation instructions."""
    schema_description = """
You are working with a SQLite database named 'sales.db' that contains the following tables:

1. sales
   - order_number: Unique ID for each order
   - line_item: Identifies individual products in an order
   - order_date: Date the order was placed
   - delivery_date: Date the order was delivered
   - customerkey: Foreign key referencing customers table
   - storekey: Foreign key referencing stores table
   - productkey: Foreign key referencing products table
   - quantity: Number of items purchased
   - currency_code: Currency used for the order

2. customers
   - customerkey: Primary key
   - gender, name, city, state_code, state, zip_code, country, continent, birthday

3. products
   - productkey: Primary key
   - product_name, brand, color, unit_cost_usd, unit_price_usd, subcategory, category

4. stores
   - storekey: Primary key
   - country, state, square_meters, open_date

5. exchange_rates
   - calender_date, currency, exchange

Your task is to generate SQL queries based on user questions. 
Use correct table joins via foreign keys:
    sales.customerkey = customers.customerkey
    sales.productkey = products.productkey
    sales.storekey = stores.storekey

Example Queries:
- "Find total sales quantity by product category"
- "List all orders placed in September 2025"
- "Get top 5 customers by total quantity ordered"
- "Find average unit price of all products"

Return only valid SQLite SQL syntax.
"""
    return schema_description.strip()


# ------------------ Run MCP Server ------------------

if __name__ == "__main__":
    mcp.run(transport="http", host="0.0.0.0", port=8000)
