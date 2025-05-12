import os
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.ingest import QueuedIngestClient
from azure.kusto.ingest.ingestion_properties import DataFormat, IngestionProperties


cluster = os.getenv("KUSTO_CLUSTER", "https://<clustername>.<region>.kusto.windows.net")
database = os.getenv("KUSTO_DATABASE", "database")
table = os.getenv("KUSTO_TABLE", "table")


# Base Functions
def get_client():
    kcsb = KustoConnectionStringBuilder.with_interactive_login(cluster)
    client = KustoClient(kcsb)
    return client


def get_ingest_client():
    kcsb = KustoConnectionStringBuilder.with_interactive_login(
        cluster.replace("https://", "https://ingest-")
    )
    client = QueuedIngestClient(kcsb)
    return client


def excute_query(client, query):
    response = client.execute(database, query) # primary_results, errors, get_raw_response, get_status, tables
    return response.primary_results[0]


def excute_ingest_query(client, table_name, data):
    ing_props = IngestionProperties(
        database        = database,
        table           = table_name,
        data_format     = DataFormat.CSV,
    )
    if isinstance(data, pd.DataFrame):
        client.ingest_from_dataframe(
            data, 
            ingestion_properties=ing_props
        )
        print("✅ Streaming ingest 완료")
    elif isinstance(data, str):
        try:
            client.ingest_from_file(
                data, 
                ingestion_properties=ing_props
            )
        except Exception as e:
            print(f"❌ Streaming ingest 실패: {e}")
            raise e
        print("🚀 파일을 큐에 넣었습니다 (백그라운드에서 적재 진행)")
    else:
        raise TypeError("data must be a pandas DataFrame or a file path string.")


# Custom Functions


if __name__ == "__main__":
    from dotenv import load_dotenv
    from datetime import datetime
    import pandas as pd 

    load_dotenv()  # load .env file in the current environment
    cluster = os.getenv("KUSTO_CLUSTER", "https://<clustername>.<region>.kusto.windows.net")
    database = os.getenv("KUSTO_DATABASE", "database")
    table = os.getenv("KUSTO_TABLE", "table")

    ingest_client = get_ingest_client()
    df = pd.DataFrame({
        "Timestamp": [datetime.utcnow(), datetime.utcnow()],
        "UserId": ["a", "v"]
    })
    excute_ingest_query(ingest_client, table, df)
    
    client = get_client()
    query = f"""{table}|take 10"""
    response = excute_query(client, query)
    print(list(response))