from flipside import Flipside

api_key = "35a503ae-8636-4d4f-98a6-0059b8306c9d"
api_url = "https://api-v2.flipsidecrypto.xyz"
flipside = Flipside(api_key, api_url)


from pyairtable import Table

def get_counts(program_id):
    response = None
    print("Fetching For: ", program_id)
    sql_query = f"""
    SELECT COUNT(DISTINCT tx_id) AS total_transactions, COUNT(distinct signers[0]) as wallets
    FROM solana.core.fact_events
    WHERE succeeded
    AND program_id = '{program_id}';
    """

    response = flipside.query(sql_query)

    data = response.records
    print(data)
    return {"tx": data[0]["total_transactions"], "wallet": data[0]["wallets"]}


base_id = 'app0Pze3RcxRYtMp6'
api_key = 'patDhtTDYJsM7B7J7.650666c0ee4f761331601ae9afaea19a01eae41f16d2b53396d5894aaaeeddbf'
table_name = 'tbldPLOYbK8Q6kdA9'

table = Table(api_key, base_id, table_name)

for record in table.all():
    program_id = record['fields']['Program ID']
    user_count = get_counts(program_id)


    data = {
        "Tx Count": user_count["tx"],
        "User Count": user_count["wallet"]
    }

    table_id = record['id']
    table.update(table_id, data)



