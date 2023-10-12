#Libraries
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sodapy import Socrata
from pyensign.events import Event
from pyensign.ensign import Ensign
import json
import asyncio
from cdp_secrets import token,ensign_client,ensign_secret

#Event Handling
async def handle_ack(ack):
    ts = datetime.fromtimestamp(ack.committed.seconds + ack.committed.nanos / 1e9)
    print(ts)

async def handle_nack(nack):
    print(f"Could not commit event {nack.id} with error {nack.code}: {nack.error}")

#CDP ETL
class ChicagoDataPublisher:
    def __init__(self, token, topic, limit=1000000000):
        self.chicago_url = "data.cityofchicago.org"
        self.vehicle_api_root = "68nd-jvt3"
        self.people_api_root = "u6pd-qa9d"
        self.crash_api_root = "85ca-t3if"
        self.limit = limit
        self.topic = topic
        self.today = datetime.now().strftime("%Y-%m-%d")
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        self.eighteen_months = (datetime.now() - relativedelta(months=18)).strftime("%Y-%m-%d")
        self.cdp = Socrata(self.chicago_url, token)

    def callAPI(self, root, query_filter):
        results = self.cdp.get(root, query=query_filter)
        return pd.DataFrame.from_records(results)

    def build_filters(self):
        vehicle_filter = f"""SELECT crash_record_id, crash_date, make, model, vehicle_type 
                             WHERE vehicle_type IS NOT NULL 
                             AND crash_date BETWEEN '{self.yesterday}' AND '{self.today}' 
                             LIMIT {self.limit}"""

        people_filter = f"""SELECT person_id, crash_record_id, crash_date, person_type, age, sex,
                            injury_classification, pedpedal_action, pedpedal_visibility, pedpedal_location
                            WHERE CRASH_DATE BETWEEN '{self.yesterday}' AND '{self.today}' 
                            AND (person_type='PEDESTRIAN' OR person_type='BICYCLE') 
                            LIMIT {self.limit}"""

        crash_filter = f"""SELECT DISTINCT *
                           WHERE crash_date BETWEEN '{self.yesterday}' AND '{self.today}' 
                           LIMIT {self.limit}"""

        return vehicle_filter, people_filter, crash_filter

    def get_data(self):
        vehicle_filter, people_filter, crash_filter = self.build_filters()
        
        people_df = self.callAPI(self.people_api_root, people_filter)
        vehicle_df = self.callAPI(self.vehicle_api_root, vehicle_filter)
        crash_df = self.callAPI(self.crash_api_root, crash_filter)

        return people_df, vehicle_df, crash_df
    
    def merge_data(self, people_df, vehicle_df, crash_df):
        people_vehicle = pd.merge(people_df, vehicle_df[['crash_record_id','make','model','vehicle_type']], on='crash_record_id', how='left')
        crash_pv = pd.merge(people_vehicle, crash_df, on='crash_record_id', how='left')

        return crash_pv

    def fetch_and_export_data(self):
        people_data, vehicle_data, crash_data = self.get_data()
        merged_data = self.merge_data(people_data, vehicle_data, crash_data)
        
        # Convert the DataFrame to a dictionary
        data_dict = merged_data.to_dict(orient='records')
        return data_dict
    

    #Load to Ensign
    async def publish(self):
        #ETL
        data_dict=self.fetch_and_export_data()

        ensign=Ensign(client_id=ensign_client,client_secret=ensign_secret)
        for record in data_dict:
            #print(record)
            event = Event(json.dumps(record).encode("utf-8"), mimetype="application/json") 
            await ensign.publish(self.topic, event, on_ack=handle_ack, on_nack=handle_nack)
        
        event = Event(json.dumps({"done":"yes"}).encode("utf-8"), mimetype="application/json")
        await ensign.publish(self.topic, event, on_ack=handle_ack, on_nack=handle_nack)
        await asyncio.sleep(10)  

    def run(self):
        asyncio.get_event_loop().run_until_complete(self.publish())



# Run the asynchronous function using asyncio
if __name__ == "__main__":
    topic='publisher_test'
    publisher=ChicagoDataPublisher(token)
    publisher.run()