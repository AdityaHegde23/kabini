import pandas as pd
import numpy as np
import itertools
from scipy.stats import expon

csv_file = 'your_data_distribution.csv'
data = pd.read_csv(csv_file)

# Define the numeric attributes you want to generate queries for
numeric_attributes = ['Global_active_power', 'Global_reactive_power', 'Voltage', 'Global_intensity',
                      'Sub_metering_1', 'Sub_metering_2', 'Sub_metering_3']

# Function to generate query centers and widths
def generate_query_parameters(attr_min, attr_max, distribution='uniform'):
    if distribution == 'uniform':
        center = np.random.randint(attr_min, attr_max + 1)
        width = np.random.randint(1, attr_max - attr_min + 1)
    else:  # Exponential distribution for data-centric
        scale = (attr_max - attr_min) / 3
        width = int(expon(scale=scale).rvs())
        center = np.random.randint(attr_min, attr_max + 1)
    return center, width

# Generate SQL queries
num_queries = 2000
queries = []

for _ in range(num_queries):
    # Randomly select the number of dimensions (attributes) for the query
    num_attrs = np.random.randint(2, len(numeric_attributes) + 1)
    selected_attrs = np.random.choice(numeric_attributes, size=num_attrs, replace=False)

    conditions = []
    for attr in selected_attrs:
        attr_min, attr_max = data[attr].min(), data[attr].max()  # Assuming min and max values are representative
        center, width = generate_query_parameters(attr_min, attr_max, distribution='uniform')
        min_val = max(attr_min, center - width // 2)
        max_val = min(attr_max, center + width // 2)
        conditions.append(f"'{attr}' BETWEEN {min_val} AND {max_val}")

    query = "SELECT * FROM your_table WHERE " + " AND ".join(conditions) + ";"
    queries.append(query)

# Optionally, save the queries to a file
with open('generated_queries.sql', 'w') as f:
    for query in queries:
        print(query)
        f.write("%s\n" % query)

print("Queries generated and saved to generated_queries.sql")
