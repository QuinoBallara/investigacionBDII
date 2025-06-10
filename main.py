import jaydebeapi
import random
import jpype
import os
import matplotlib.pyplot as plt
import time
import numpy as np

# JARs que querés usar
JAR_PATHS = [
    "C:/Users/Quino/Downloads/db2jdbcdriver/jcc-12.1.0.0.jar"
]
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

# Sample data
COUNTRIES = ['UY', 'AR', 'BR']
NAMES = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eva', 'Frank', 'Grace', 'Henry']
USER_AMOUNT = 10000000  # Total number of users to generate
TEST_AMOUNT = 20 # Number of iterations for the benchmark

# Function to generate users
def generate_users(n):
    print(f"Generating {n} users...")
    users = []
    for i in range(1, n + 1):
        name = random.choice(NAMES)
        age = random.randint(5, 99)
        country = random.choice(COUNTRIES)
        users.append((i, name, age, country))
    return users

# Connect to DB2
conn = jaydebeapi.connect(JDBC_DRIVER_CLASS, JDBC_URL, [USERNAME, PASSWORD])
cursor = conn.cursor()

def delete_users_in_tables(cursor):
    print("Deleting existing tables...")
    cursor.execute("DELETE FROM users_range")
    cursor.execute("DELETE FROM users_list")
    cursor.execute("DELETE FROM users")

def insert_users_range(cursor, users):
    print("Inserting into users_range...")
    start = time.perf_counter()
    for user in users:
        cursor.execute("INSERT INTO users_range (id, name, age, country_code) VALUES (?, ?, ?, ?)", user)
    end = time.perf_counter()
    return end - start

def insert_users_list(cursor, users):
    print("Inserting into users_list...")
    start = time.perf_counter()
    for user in users:
        cursor.execute("INSERT INTO users_list (id, name, age, country_code) VALUES (?, ?, ?, ?)", user)
    end = time.perf_counter()
    return end - start

def insert_users(cursor, users):
    print("Inserting into users...")
    start = time.perf_counter()
    for user in users:
        cursor.execute("INSERT INTO users (id, name, age, country_code) VALUES (?, ?, ?, ?)", user)
    end = time.perf_counter()
    return end - start


def create_bar_graph(timeRange, timeList, timeUsers):
    table_names = ['Particiones por rango de edad', 'Particiones por lista de país', 'Sin particiones']
    average_times = [timeRange, timeList, timeUsers]

    plt.figure(figsize=(8, 5))
    bars = plt.bar(table_names, average_times, color=['lightgreen', 'orange', 'skyblue'])

    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2.0, yval + 0.01, f'{yval:.2f}', ha='center', va='bottom')

    plt.title(f'Tiempo promedio de inserción de {USER_AMOUNT} elementos por tabla ({TEST_AMOUNT} iteraciones)')
    plt.ylabel('Tiempo (segundos)')
    plt.ylim(0, max(average_times) * 1.2)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig("insercion_pro.png")
    plt.close()

def benchmark_and_insert(cursor):
    tables = ['users', 'users_list', 'users_range']
    queries = {
        'País = UY': "SELECT * FROM {table} WHERE country_code = 'UY'",
        'Edades entre 27-49': "SELECT * FROM {table} WHERE age BETWEEN 27 AND 49",
        'Sin restricciones': "SELECT * FROM {table} WHERE name = 'Alice' OR name = 'Bob'" 
    }

    total_time = {table: 0 for table in tables}
    results = {query_name: {table: 0 for table in tables} for query_name in queries}

    for i in range(TEST_AMOUNT):
        print(f"\n--- Iteration {i + 1}/{TEST_AMOUNT} ---")
        users = generate_users(USER_AMOUNT)
        delete_users_in_tables(cursor)

        conn.commit()

        # Insert and time
        timeRange = insert_users_range(cursor, users)
        timeList = insert_users_list(cursor, users)
        timeUsers = insert_users(cursor, users)

        total_time['users_range'] += timeRange
        total_time['users_list'] += timeList
        total_time['users'] += timeUsers

        conn.commit()

        # Query and time
        for query_name, query in queries.items():
            for table in tables:
                q_start = time.perf_counter()
                cursor.execute(query.format(table=table))
                cursor.fetchall()
                q_end = time.perf_counter()
                results[query_name][table] += (q_end - q_start)

    avg_time = {table: total_time[table] / TEST_AMOUNT for table in tables}
    for query_name in results:
        for table in results[query_name]:
            results[query_name][table] /= TEST_AMOUNT

    conn.commit()

    return avg_time, results

def plot_query_results(results):
    table_name_map = {
        'users': 'Sin particiones',
        'users_list': 'Particiones por lista de país',
        'users_range': 'Particiones por rango de edad'
    }

    queries = list(results.keys())
    tables = list(results[queries[0]].keys())
    friendly_tables = [table_name_map[table] for table in tables]

    data = [[results[query][table] for table in tables] for query in queries]

    x = np.arange(len(tables))
    width = 0.25

    plt.figure(figsize=(10, 6))

    for i, query in enumerate(queries):
        plt.bar(x + i * width, data[i], width, label=query)

    plt.ylabel('Tiempo promedio por consulta (s)')
    plt.title(f'Tiempo promedio de consulta por tabla y condición ({TEST_AMOUNT} iteraciones y {USER_AMOUNT} elementos)')
    plt.xticks(x + width, friendly_tables)
    plt.legend()
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig("consultas_pro.png")
    plt.close()

# Main execution
avg_time, results = benchmark_and_insert(cursor)
print(f"\n--- Promedios de Inserción ---")
print(f"users_range: {avg_time['users_range']:.2f} s")
print(f"users_list:  {avg_time['users_list']:.2f} s")
print(f"users:       {avg_time['users']:.2f} s")

create_bar_graph(avg_time['users_range'], avg_time['users_list'], avg_time['users'])
plot_query_results(results)

# Print average query times as a table
print("\n--- Promedios de Consulta (segundos) ---")
for query_name, table_times in results.items():
    print(f"\n{query_name}:")
    for table, avg_query_time in table_times.items():
        print(f"  {table}: {avg_query_time:.4f} s")

delete_users_in_tables(cursor)

conn.commit()
cursor.close()
conn.close()

print("Experimento completado.")
