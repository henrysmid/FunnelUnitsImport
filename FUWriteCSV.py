# Import CSV files into Salesforce propagation row based the data into FUCSVQueue__c
# Enhancement verification before import (6th Sep 2024)
# Author: Henry Smid
# Last Update: September 2024

import json
import time
from datetime import datetime
from typing import OrderedDict

from salesforce_bulk import SalesforceBulk
from simple_salesforce import Salesforce



def get_current_timestamp():
    current_utc_timestamp = datetime.now()

    # Convert to string if needed
    utc_timestamp_str = current_utc_timestamp.strftime("%Y-%m-%d %H:%M:%S")

    return utc_timestamp_str


def post_batch_and_monitor(job, bulk, records_json):
    is_ok = True
    ts = get_current_timestamp()
    # Post the batch
    batch = bulk.post_batch(job, records_json)
    print(f"{ts} - Batch {batch} submitted.")
    batch_status = None
    delay = 5
    max_waiting_loops = 12
    loop_counter = 0

    # Monitor the batch until it is completed
    while True:
        ts = get_current_timestamp()
        if loop_counter > max_waiting_loops:
            print(f"{ts} - Batch {batch} canceled after {max_waiting_loops}")
            is_ok = False
            batch_status = {
                "state": "Failed"
            }
            break

        try:
            batch_status = bulk.batch_status(batch, job, True)
            state = batch_status['state']
            state_msg = batch_status['stateMessage']
            processing_time = batch_status['totalProcessingTime']
            rec_processed = batch_status['numberRecordsProcessed']
            rec_failed = batch_status['numberRecordsFailed']

            # Check if the batch is in progress
            if state == 'InProgress':
                print(f"{ts} - Batch {batch} status: {state}, processing time: {processing_time} - records processed: {rec_processed} - records failed: {rec_failed} - message: {state_msg}")

            # Check if the batch is completed, failed, or aborted
            elif state in ['Completed', 'Failed', 'Not Processed']:
                if state == 'Failed':
                    print(f"{ts} - Batch {batch} failed with errors: {state_msg}")
                    is_ok = False
                else:
                    print(f"{ts} - Batch {batch} finished with state: {state}.")
                break
            else:
                print(f"{ts} - Batch {batch} with status: {state} and message: {state_msg}")

        except Exception as e:
            print(f"{ts} - Error occurred while checking batch status: {str(e)}")
            break

        # Wait before polling again to avoid excessive requests
        loop_counter += 1
        time.sleep(delay)  # 10 seconds delay
    return batch_status

#verify whether in the target SObject all objects are deleted for the name e.g. Account
def verify_clean_target(sf, name):
    is_ok = False

    qry = 'SELECT Count() FROM FUCSVQueue__c WHERE Name =' + '\'' + name + '\''
    try:
        result = sf.query(qry)
        ts = get_current_timestamp()
        print(f"{ts} - result of the verification {qry} is: {result}")
        size = result['totalSize']
        if size == 0:
            is_ok = True
        else:
            print(f"{ts} - import cannot be processed because the target object {name} has already data: {size} records")
    except Exception as e:
        ts = get_current_timestamp()
        print(f"{ts} - Error occurred while verification with query {qry} status: {str(e)}")

    return is_ok

def post_to_salesforce(file_name, name):
    cnt = 0
    i = 0
    tag = 'Acrolinx'
    records = []
    max = 1000
    limit = 0

    # Establish connection to Salesforce
    username = ''
    password = ''
    security_token = ''
    sandbox = False
    ts = get_current_timestamp()
    print(f"{ts} - create connection")

    sf = Salesforce(username=username, password=password, security_token=security_token)

    # Expose the limits
    limits = sf.limits()
    ts = get_current_timestamp()
    print(f"{ts} - limits: {limits}")

    if verify_clean_target(sf, name):

        # Extract the session ID and instance URL for SalesforceBulk
        session_id = sf.session_id
        instance_url = sf.sf_instance

        # Use the session ID and instance URL to create a SalesforceBulk instance
        bulk = SalesforceBulk(sessionId=session_id, host=instance_url)

        ts = get_current_timestamp()
        print(f"{ts} - create connection done")

        # Create a new job for the insert operation
        job = bulk.create_insert_job('FUCSVQueue__c', contentType='JSON')

        ts = get_current_timestamp()
        print(f"{ts} -  job {job} created.")

        with open(file_name, 'r', encoding='latin-1') as file:
            # Iterate over each line in the file
            for line in file:
                i += 1
                # Each 'line' includes a newline character at the end, you can strip it using strip()
                # print(line.strip())
                records.append({'Name': name, 'Tag__c': tag, 'Row__c': cnt + i - 1, 'Values__c': line.strip()})
                is_ok = True
                if i == max:
                    # Add the batch - note that we're passing the records directly
                    records_json = json.dumps(records)
                    batch_status = post_batch_and_monitor(job, bulk, records_json)
                    state = batch_status['state']
                    if state == 'Failed':
                        is_ok = False
                    else :
                        cnt = cnt + i
                        ts = get_current_timestamp()
                        print(f"{ts} - {cnt} objects processed so far")
                    i = 0
                    records = []
                if not is_ok or ((limit > 0) and (cnt >= limit)):
                    break

            # process the last chunk
            if i > 0:
                records_json = json.dumps(records)
                post_batch_and_monitor(job, bulk, records_json)
            cnt = cnt + i
            ts = get_current_timestamp()
            print(f"{ts} - Batch finalized: {cnt} objects processed in total")

        try:
            bulk.close_job(job)
            ts = get_current_timestamp()
            print(f"{ts} - Job {job} closed")

        except Exception as e:
            ts = get_current_timestamp()
            print(f"{ts} - Error occurred while closing {job} status: {str(e)}")


# ========================================================================================

file_path = 'C:\\Users\\henry\\Dropbox\\IT\\Funnel Units\\Test\\Data\\Acrolinx\\'
file_path_accounts = file_path + 'Accounts 02-2024.csv'
file_path_opps = file_path + 'Opportunities_10_Opp.csv'
file_path_history = file_path + 'OpportunityHistorie_10_Opp_Neu.csv'

name_account = 'Account'
name_opp = 'Opportunity'
name_history = 'OpportunityHistory'

post_to_salesforce(file_path_accounts, name_account)

