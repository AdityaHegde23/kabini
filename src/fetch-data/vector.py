import pandas as pd
import numpy as np
import fetch_data


vector_dump_path = "../utils/forestcovertype/forestcovertype_"

numeric_attributes = fetch_data.df.columns.tolist()

queries_count = 200

vector_data_columns = [
    f"{col}_{suffix}" for col in numeric_attributes for suffix in ["start", "end"]
]
vector_data_distb = [(0,) * len(vector_data_columns)] * (queries_count//2)
vector_df_distb = pd.DataFrame(vector_data_distb, columns=vector_data_columns)

vector_data_tupb = [(0,) * len(vector_data_columns)] * (queries_count//2)
vector_df_tupb = pd.DataFrame(vector_data_tupb, columns=vector_data_columns)



def generate_distribution_based_query(df, attributes, row_number):
    conditions = []
    for attr in attributes:
        attr_min, attr_max = df[attr].min(), df[attr].max()
        value1 = np.random.uniform(attr_min, attr_max)
        value2 = np.random.uniform(attr_min, attr_max)

        minValue = min(value1, value2)
        maxValue = max(value2, value1)
        conditions.append(f"\"{attr}\" BETWEEN {minValue:.2f} AND {maxValue:.2f}")

        vector_df_distb.at[row_number, f"{attr}_start"] = minValue
        vector_df_distb.at[row_number, f"{attr}_end"] = maxValue
    #print(conditions)
    return "SELECT * FROM your_table WHERE " + " AND ".join(conditions) + ";"

def generate_tuple_based_query(df, attributes, row_number):
    sample = df.sample(1, random_state=42)
    sample2 = df.sample(1, random_state=43)
    conditions = []
    for attr in attributes:
      minValue = min(sample.iloc[0][attr], sample2.iloc[0][attr])
      maxValue = max(sample.iloc[0][attr], sample2.iloc[0][attr])
      conditions.append(f"\"{attr}\" BETWEEN {minValue:.2f} AND {maxValue:.2f}")


      vector_df_tupb.at[row_number, f"{attr}_start"] = minValue
      vector_df_tupb.at[row_number, f"{attr}_end"] = maxValue
    return "SELECT * FROM your_table WHERE " + " AND ".join(conditions) + ";"

# Generate SQL queries
#num_queries = 6  # Example number of queries to generate
queries = []

for row_number in range(queries_count // 2):  # Half based on distribution
    num_attrs = np.random.randint(2, len(numeric_attributes) + 1)
    selected_attrs = np.random.choice(numeric_attributes, size=num_attrs, replace=False)
    query = generate_distribution_based_query(fetch_data.df, selected_attrs, row_number)
    queries.append(query)

for row_number in range(queries_count // 2):  # Half based on data tuples
    num_attrs = np.random.randint(2, len(numeric_attributes) + 1)
    selected_attrs = np.random.choice(numeric_attributes, size=num_attrs, replace=False)
    query = generate_tuple_based_query(fetch_data.df, selected_attrs, row_number)
    queries.append(query)

vector_df = pd.concat([vector_df_distb,vector_df_tupb])
vector_df.to_csv('vector.csv',index=False, header=False)

# Example: Print the first few queries
# for query in queries[:5]:
#     print(query)

queries_df = pd.DataFrame(queries)
queries_df.to_csv('queries.csv')

