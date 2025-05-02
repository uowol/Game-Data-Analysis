from google.cloud import bigquery


# Base Functions
def get_client(auth_file=None):
    client = bigquery.Client.from_service_account_json(auth_file) if auth_file else bigquery.Client()
    return client

def excute_query(client, query, job_config=None):
    query_job = None
    if job_config:
        query_job = client.query(query, job_config=job_config)
    else:
        query_job = client.query(query)
    return query_job.result()


# Custom Functions
def create_insert_query(table_name, columns, primary_keys):
    non_pk_columns = [col for col in columns if col not in primary_keys]
    
    merge_query = f"""
    MERGE `{table_name}` T
    USING (SELECT {', '.join(['@' + col + f' AS {col}' for col in columns])}) S
    ON {' AND '.join([f'T.{pk} = S.{pk}' for pk in primary_keys])}
    WHEN MATCHED THEN UPDATE SET {', '.join([f'{col} = S.{col}' for col in non_pk_columns])}
    WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) VALUES ({', '.join([f'S.{col}' for col in columns])});
    """
    return merge_query

def create_job_config(data: list):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(title, content_format, content)
            for title, content_format, content in data
        ]
    )
    return job_config


def create_dataset(client, dataset_id):
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset = client.create_dataset(dataset, exists_ok=True)
    return dataset

# if __name__ == "__main__":
#     conn = get_connection("outputs/duckdb.db")
#     df = conn.execute("SELECT * FROM read_json_auto('modules/data_ingestion/output/league.json')").fetchdf()
#     print(df)
