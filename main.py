import jaydebeapi
import random
import jpype
import os
import matplotlib.pyplot as plt
import time

# JARs que querés usar
JAR_PATHS = [
    "C:/Users/Quino/Downloads/db2jdbcdriver/jcc-12.1.0.0.jar"
]

# Convertilos a una única string separada por ';' (en Windows)
classpath = ";".join(JAR_PATHS)

# Solo arrancar la JVM si no está corriendo
if not jpype.isJVMStarted():
    jpype.startJVM(
        "C:/Program Files/Java/jdk-17/bin/server/jvm.dll",  # ruta a tu JVM
        "-Djava.class.path=" + classpath
    )



# DB2 JDBC connection details
JDBC_URL = "jdbc:db2://192.168.56.101:50000/research"
USERNAME = "quino"
PASSWORD = "quino"
JDBC_DRIVER_CLASS = "com.ibm.db2.jcc.DB2Driver"
JAR_FILES = r"C:\Users\Quino\Downloads\db2jdbcdriver\jcc-12.1.0.0.jar;C:\Users\Quino\Downloads\db2jdbcdriver\jt400-21.0.3.jar"

# Sample data
COUNTRIES = ['UY', 'AR', 'BR']
NAMES = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eva', 'Frank', 'Grace', 'Henry']
USER_AMOUNT = 100
TEST_AMOUNT = 1

# Generate users
users = []
for i in range(1, USER_AMOUNT + 1):
    name = random.choice(NAMES) + str(i)
    age = random.randint(10, 90)
    country = random.choice(COUNTRIES)
    users.append((i, name, age, country))

# Connect and insert data
conn = jaydebeapi.connect(JDBC_DRIVER_CLASS, JDBC_URL, [USERNAME, PASSWORD])
cursor = conn.cursor()

def delete_users_in_tables(cursor):
    """Delete existing tables if they exist."""
    print("Deleting existing tables...")
    cursor.execute("DELETE FROM users_range")
    cursor.execute("DELETE FROM users_list")
    cursor.execute("DELETE FROM users")

def insert_users_range(cursor, users):
    """Insert users into users_range table."""
    print("Inserting into users_range...")
    start = time.perf_counter()
    for user in users:
        cursor.execute("INSERT INTO users_range (id, name, age, country_code) VALUES (?, ?, ?, ?)", user)
    end = time.perf_counter()
    timeRange = end - start
    return timeRange

def insert_users_list(cursor, users):
    """Insert users into users_list table."""
    print("Inserting into users_list...")
    start = time.perf_counter()
    for user in users:
        cursor.execute("INSERT INTO users_list (id, name, age, country_code) VALUES (?, ?, ?, ?)", user)
    end = time.perf_counter()
    timeList = end - start
    return timeList

def insert_users(cursor, users):
    """Insert users into users table."""
    print("Inserting into users...")
    start = time.perf_counter()
    for user in users:
        cursor.execute("INSERT INTO users (id, name, age, country_code) VALUES (?, ?, ?, ?)", user)
    end = time.perf_counter()
    timeUsers = end - start
    return timeUsers

def create_bar_graph(timeRange, timeList, timeUsers):
    # Data
    table_names = ['users_range', 'users_list', 'users']
    average_times = [timeRange, timeList, timeUsers]

    # Plot
    plt.figure(figsize=(8, 5))
    bars = plt.bar(table_names, average_times, color=['skyblue', 'orange', 'lightgreen'])

    # Add values on top of bars
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2.0, yval + 0.01, f'{yval:.2f}', ha='center', va='bottom')

    plt.title(f'Average Insert Time per Table ({TEST_AMOUNT} iterations)')
    plt.ylabel('Time (seconds)')
    plt.xlabel('Table Name')
    plt.ylim(0, max(average_times) * 1.2)
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    plt.tight_layout()
    plt.show()

def benchmark_queries(cursor, users):
    tables = ['users', 'users_list', 'users_range']
    queries = {
        'Country = UY': "SELECT * FROM {table} WHERE country_code = 'UY'",
        'Age 27-49': "SELECT * FROM {table} WHERE age BETWEEN 27 AND 49",
        'Full Table': "SELECT * FROM {table}"
    }

    results = {query_name: {table: 0 for table in tables} for query_name in queries}

    for _ in range(TEST_AMOUNT):
        delete_users_in_tables(cursor)
        insert_users_range(cursor, users)
        insert_users_list(cursor, users)
        insert_users(cursor, users)
        for query_name, query in queries.items():
            for table in tables:
                start = time.perf_counter()
                cursor.execute(query.format(table=table))
                cursor.fetchall()
                end = time.perf_counter()
                results[query_name][table] += (end - start)

    # Compute averages
    for query_name in results:
        for table in results[query_name]:
            results[query_name][table] /= TEST_AMOUNT

    return results

def plot_query_results(results):
    import numpy as np

    queries = list(results.keys())
    tables = list(results[queries[0]].keys())

    # Create data matrix: rows are queries, columns are tables
    data = [[results[query][table] for table in tables] for query in queries]

    x = np.arange(len(tables))  # group positions
    width = 0.25

    plt.figure(figsize=(10, 6))

    for i, query in enumerate(queries):
        plt.bar(x + i * width, data[i], width, label=query)

    plt.xlabel('Table')
    plt.ylabel('Average Query Time (s)')
    plt.title(f'Average Query Time by Table and Condition ({TEST_AMOUNT} Iterations)')
    plt.xticks(x + width, tables)
    plt.legend()
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()


# Main execution
total_time_users = 0
total_time_users_list = 0
total_time_users_range = 0
for i in range(TEST_AMOUNT):
    print(f"Iteration {i + 1}/{TEST_AMOUNT}")
    delete_users_in_tables(cursor)
    timeRange = insert_users_range(cursor, users)
    timeList = insert_users_list(cursor, users)
    timeUsers = insert_users(cursor, users)

    total_time_users_range += timeRange
    total_time_users_list += timeList
    total_time_users += timeUsers

# Calculate average times
timeRange = total_time_users_range / TEST_AMOUNT
timeList = total_time_users_list / TEST_AMOUNT
timeUsers = total_time_users / TEST_AMOUNT

print(f"Time taken for users_range: {timeRange:.2f} seconds")
print(f"Time taken for users_list: {timeList:.2f} seconds")
print(f"Time taken for users: {timeUsers:.2f} seconds")

create_bar_graph(timeRange, timeList, timeUsers)

# Benchmark queries
results = benchmark_queries(cursor, users)
plot_query_results(results)

delete_users_in_tables(cursor)

# Commit changes and close connection
conn.commit()
cursor.close()
conn.close()

print("Data inserted into both tables.")
