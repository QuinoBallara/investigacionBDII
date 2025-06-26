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

USER_AMOUNT = 1000000  # Total number of users to generate
TEST_AMOUNT = 5 # Number of iterations for the benchmark
BATCH_SIZE = 10000  # Size of each batch for insertion

# Function to generate users randomly
def generate_users(n):
    print(f"Generating {n} users...")
    users = []
    for i in range(1, n + 1):
        name = random.choice(NAMES)
        age = random.randint(5, 99)
        country = random.choice(COUNTRIES)
        users.append((i, name, age, country))
    return users

# Function to generate users evenly distributed across age ranges and countries
def generate_users_evenly(n):
    print(f"Generating {n} users...")
    users = []
    
    # Age groups: 0-25, 26-50, 51-150
    age_groups = [
        (5, 25),    # Young (using 5 as minimum instead of 0)
        (26, 50),   # Middle
        (51, 99)    # Senior (using 99 as maximum instead of 150)
    ]
    
    countries = COUNTRIES  # ['UY', 'AR', 'BR']
    
    # Calculate users per country and per age group
    users_per_country = n // len(countries)
    users_per_age_group = users_per_country // len(age_groups)
    
    user_id = 1
    
    for country in countries:
        for age_min, age_max in age_groups:
            # Generate users for this country and age group
            for i in range(users_per_age_group):
                if user_id > n:  # Don't exceed the requested number
                    break
                    
                name = NAMES[(user_id - 1) % len(NAMES)]  # Cycle through names
                # Distribute ages evenly within the range
                age_range = age_max - age_min + 1
                age = age_min + (i % age_range)
                
                users.append((user_id, name, age, country))
                user_id += 1
    
    # Handle any remaining users due to rounding
    remaining = n - (user_id - 1)
    for i in range(remaining):
        if user_id > n:
            break
        name = NAMES[(user_id - 1) % len(NAMES)]
        age = 5 + (i % 95)  # Distribute remaining ages
        country = countries[i % len(countries)]
        users.append((user_id, name, age, country))
        user_id += 1
    
    return users

# Connect to DB2
conn = jaydebeapi.connect(JDBC_DRIVER_CLASS, JDBC_URL, [USERNAME, PASSWORD])
cursor = conn.cursor()

def delete_users_in_tables(cursor):
    print("Deleting existing tables...")
    cursor.execute("DELETE FROM users_range")
    conn.commit()  
    cursor.execute("DELETE FROM users_list")
    conn.commit()
    cursor.execute("DELETE FROM users")
    conn.commit()

def insert_users_range(cursor, users):
    print("Inserting into users_range...")
    start = time.perf_counter()

    for i in range(0, len(users), BATCH_SIZE):
        batch = users[i:i + BATCH_SIZE]
        cursor.executemany("INSERT INTO users_range (id, name, age, country_code) VALUES (?, ?, ?, ?)", batch)
        conn.commit()
        print(f"Committed batch {i//BATCH_SIZE + 1}: rows {i+1} to {min(i+BATCH_SIZE, len(users))}")
    
    end = time.perf_counter()
    return end - start

def insert_users_list(cursor, users):
    print("Inserting into users_list...")
    start = time.perf_counter()
    
    for i in range(0, len(users), BATCH_SIZE):
        batch = users[i:i + BATCH_SIZE]
        cursor.executemany("INSERT INTO users_list (id, name, age, country_code) VALUES (?, ?, ?, ?)", batch)
        conn.commit()
        print(f"Committed batch {i//BATCH_SIZE + 1}: rows {i+1} to {min(i+BATCH_SIZE, len(users))}")
    
    end = time.perf_counter()
    return end - start

def insert_users(cursor, users):
    print("Inserting into users...")
    start = time.perf_counter()
    
    for i in range(0, len(users), BATCH_SIZE):
        batch = users[i:i + BATCH_SIZE]
        cursor.executemany("INSERT INTO users (id, name, age, country_code) VALUES (?, ?, ?, ?)", batch)
        conn.commit()
        print(f"Committed batch {i//BATCH_SIZE + 1}: rows {i+1} to {min(i+BATCH_SIZE, len(users))}")
    
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
    conn.commit()
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
        users = generate_users_evenly(USER_AMOUNT)

        conn.commit()  # Commit deletion before insertion

        # Insert and time
        timeRange = insert_users_range(cursor, users)
        timeList = insert_users_list(cursor, users)
        timeUsers = insert_users(cursor, users)

        total_time['users_range'] += timeRange
        total_time['users_list'] += timeList
        total_time['users'] += timeUsers

        # Query and time
        for query_name, query in queries.items():
            for table in tables:
                q_start = time.perf_counter()
                cursor.execute(query.format(table=table))
                cursor.fetchall()
                q_end = time.perf_counter()
                results[query_name][table] += (q_end - q_start)
        
        conn.commit()  # Commit after queries

    avg_time = {table: total_time[table] / TEST_AMOUNT for table in tables}
    for query_name in results:
        for table in results[query_name]:
            results[query_name][table] /= TEST_AMOUNT

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
cursor.close()
conn.close()

print("Experimento completado.")
