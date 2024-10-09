import streamlit as st
import pymysql
import pandas as pd

# Connect to the analytics database
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='rootpassword',
    database='analytics'
)

# Query the transformed data
query = "SELECT * FROM transformed_inventory"
data = pd.read_sql(query, connection)

# Display the data in a Streamlit app
st.title("Inventory Analytics")
st.write("Transformed Inventory Data")

st.dataframe(data)

# Show some analysis
total_value = data['total_value'].sum()
st.metric(label="Total Inventory Value", value=f"${total_value}")
