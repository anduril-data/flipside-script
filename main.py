import pytz
import schedule
import time
from datetime import datetime, timedelta
from flipside import Flipside
from pyairtable import Table

api_key = "35a503ae-8636-4d4f-98a6-0059b8306c9d"
api_url = "https://api-v2.flipsidecrypto.xyz"
flipside = Flipside(api_key, api_url)

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

def main():
    base_id = 'appikR566Vy7HkZlx'
    api_key = 'patTDS12MBTJ9jsnJ.44153b8673259c9f9ccfc84d21c4e7b60e8fef06ad5c924c20414441dc5354fa'
    table_name = 'tblXXJwTt9qiBpV6D'

    table = Table(api_key, base_id, table_name)

    for record in table.all():
        print(record)
        try:
            program_id = record['fields']['Program ID']
            user_count = get_counts(program_id)

            data = {
                "Txn Count": user_count["tx"],
                "Wallet Count": user_count["wallet"]
            }

            table_id = record['id']
            table.update(table_id, data)
        except:
            continue

# Run the main function once at the beginning
main()

def job():
    # Convert current time to IST
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now()
    current_time_ist = current_time.astimezone(ist)

    # Check if it's Thursday at 7 AM IST
    if current_time_ist.weekday() == 3 and current_time_ist.hour == 7:
        main()

# Schedule the job to run
schedule.every().day.at('00:00').do(job)

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(1)
