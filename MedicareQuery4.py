import pandas as pd
from google.cloud import bigquery
import pyarrow


#SQL code to query the data from the the cloud
QUERY = """
        SELECT provider_city, provider_state, drg_definition,
        average_total_payments, average_medicare_payments
        FROM `bigquery-public-data.cms_medicare.inpatient_charges_2015`
        WHERE drg_definition LIKE '554 %'
        ORDER BY provider_city ASC
        LIMIT 1000
        """

client = bigquery.Client.from_service_account_json(
    r'C:\Users\admin\Documents\AIO Python\BigData Project\medicareproject-339417-98e1c8a76351.json')
query_job = client.query(QUERY)

#dataframe the query
df = query_job.to_dataframe()

states = df.provider_state.unique()
states.sort()

# How much Medicare pays on average for 554 DRG patients?
totalpayments = df.average_total_payments.sum()
totalmedpayments = df.average_medicare_payments.sum()

percentpaid = (totalmedpayments/totalpayments)*100

print("Overall:")
print(f"Medicare pays {percentpaid:.2f} % for 554 DRG")
print(f"Patient pays {(100 - percentpaid):.2f} % for 554 DRG")


#How much do Medicare pays on average for 554 DRG patients in each state?
print('Per State:')
print(df.head(5))
state_percent = []

for curr in states:
    state_df = df[df.provider_state == curr]
    state_total_pay = state_df.average_total_payments.sum()
    state_med_total_pay = state_df.average_medicare_payments.sum()
    percentpaid = (state_med_total_pay/state_total_pay)*100
    state_percent.append(percentpaid)
    print(f"Medicare pays {percentpaid:.2f} of Total for 554 DRG in {curr}")