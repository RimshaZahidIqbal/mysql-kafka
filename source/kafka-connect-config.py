import requests
import json

# Kafka Connect REST API URL
KAFKA_CONNECT_URL = 'http://localhost:8083/connectors'

def create_mysql_source_connector():
    # The connector configuration
    connector_config = {
        "name": "mysql-source-connector",
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "tasks.max": "1",
            "connection.url": "jdbc:mysql://mysql:3306/my_database?user=root&password=rootpassword",
            "table.whitelist": "productlines,products,offices,employees,customers,payments,orders,orderdetails",
            "mode": "incrementing", 
            "incrementing.column.name": "id",
            "topic.prefix": "mysql-",
            "poll.interval.ms": "1000"
        }
    }
    
    # Sending request to Kafka Connect to create a new connector
    headers = {"Content-Type": "application/json"}
    response = requests.post(KAFKA_CONNECT_URL, headers=headers, data=json.dumps(connector_config))
    
    if response.status_code == 201:
        print("Connector created successfully!")
    else:
        print(f"Failed to create connector: {response.text}")

if __name__ == "__main__":
    create_mysql_source_connector()
