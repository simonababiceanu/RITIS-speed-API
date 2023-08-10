import sys
import requests
import uuid
import time
import os
from datetime import datetime
import openpyxl
from pathlib import Path
import csv
import pip_system_certs.wrapt_requests

# This is the API key to login into RITIS
RITIS_API_KEY = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

# Sleep this many seconds after the POST request has been successfully generated until checking on the status of the job
SECONDS_TO_SLEEP = 120

# This is the file containing the list of TMCs needed. I have this from Katie
tmcFilePath = '.'
tmcFileName = 'TMCpaths.xlsx'

# This file stores the date for which the code was run previously. The code will run starting the day after that up until and including yesterday
last_run_filename = "last_run.txt"

# HTTP Response Codes
status_code_OK = 200

# RITIS job status codes
job_status_SUCCEEDED = 'SUCCEEDED'
job_status_UNDEFINED = 'UNDEFINED'
job_status_FAILED = 'FAILED'
job_status_KILLED = 'KILLED'

# Generate a random UUID for this request that will be used to retrieve the results when the query is done
curr_uuid = str(uuid.uuid4())  

## Output of this entire process is stored in this file
outZIPfolder = '.'
outZIPfilename = 'daily_'
outZIPextension = '.zip'

#############################################################################
    ##
    ## Preparatory steps: Retrieve the list of TMCs and the first and last date to be used in the query
    ##
    #########################################################################

# create the list of TMCs
tmcs = []
# Open the file with the list of TMCs 
xlsx_file = Path(tmcFilePath, tmcFileName)
wb_obj = openpyxl.load_workbook(xlsx_file) 
sheet = wb_obj.active
# Retrieved number of rows (1 + number of TMCs)
num_rows = sheet.max_row
# ignore first row, it is headers
for row in range(2, num_rows+1):
    tmc = sheet["A" + str(row)].value
    tmcs.append(tmc)

# Figure out when the code was last run
last_run_file = open(last_run_filename, 'r')
last_run_fileReader = csv.reader(last_run_file)
last_run_str = (list(last_run_fileReader))[0][0]
last_run = datetime.strptime(last_run_str, "%Y-%m-%d")
last_run_file.close()

# first date to run is last run date minus 1. At the end, the code writes today's date in the last run file
# Since the endDate is exclusive, the request will not retrieve data for this date, so we have to start there.
start_date = last_run
start_year_str = str(start_date.year)
if start_date.month < 10:
    start_month_str = '0' + str(start_date.month)
else:
    start_month_str = str(start_date.month)
if start_date.day < 10:
    start_day_str = '0' + str(start_date.day)
else:
    start_day_str = str(start_date.day)
startDate = start_year_str + "-" + start_month_str + "-" + start_day_str

# last date to run is yesterday. Retrieve today's date from the system
# Since last date is exclusive, use this
end_date = datetime.today().date()
end_year_str = str(end_date.year)
if end_date.month < 10:
    end_month_str = '0' + str(end_date.month)
else:
    end_month_str = str(end_date.month)
if end_date.day < 10:
    end_day_str = '0' + str(end_date.day)
else:
    end_day_str = str(end_date.day)
endDate = end_year_str + "-" + end_month_str + "-" + end_day_str

# Ensure that there is at least one day between startDate and endDate
if startDate >= endDate:
    print('ERROR: startDate should be before endDate\n\n')
    exit()


#############################################################################
    ##
    ## First step: Submit a job for data using a POST request.
    ## Define what data we want to retrieve back
    ##
    #########################################################################

# The following are for generating the job submission POST request
addNullRecords = True
averagingWindow = 0
columns = ["SPEED"]
dataSource = "vpp_inrix"
includeIncalculable = True
maxQualityFilter = 1
minQualityFilter = 0
thresholds = [30,20,10]
dows = [0,1,2,3,4,5,6]
granularityType = "minutes"
granularityValue = 15
mergeFiles = True
endTimes = "23:59:59.000"
startTimes = "00:00:00.000"
travelTimeUnits = "SECONDS"

# Generate RITIS URL for the POST request
submit_export_url = "http://pda-api.ritis.org:8080/submit/export?key=" + RITIS_API_KEY
# Generate the RITIS data request JSON payload for the POST request
submit_export_json = {
    "addNullRecords": addNullRecords,
    "averagingWindow": averagingWindow,
    "dataSourceFields": [{
            "columns": columns,
             "dataSource": dataSource,
             "qualityFilter": {
                     "includeIncalculable": includeIncalculable,
                      "max": maxQualityFilter,
                      "min": minQualityFilter,
                      "thresholds":thresholds
                      }
                      }
                     ],
    "dates": [{
            "end": endDate,
            "start": startDate
              }
             ],
    "dow": dows,
    "dsFields": [{
            "columns": columns,
            "dataSource": dataSource,
            "qualityFilter": {
                    "includeIncalculable": includeIncalculable,
                    "max": maxQualityFilter,
                    "min": minQualityFilter,
                    "thresholds": thresholds
                    }
                 }
                ],
    "granularity": {
            "type": granularityType,
            "value": granularityValue
            },
    "mergeFiles": mergeFiles,
    "times":[{
            "end": endTimes,
            "start": startTimes
             }
            ],
    "tmcs": tmcs,
    "travelTimeUnits": travelTimeUnits,
    "uuid": curr_uuid
}
    
# Send the POST request
ritis_response = requests.post(submit_export_url, json = submit_export_json, verify = False)
print('Data payload was ', submit_export_json, '\n\n')
# Capture the status code of the response from RITIS
ritis_status_code = ritis_response.status_code
if ritis_status_code == status_code_OK: 
    ritis_response_jobID = ritis_response.json()['id']
    print('Job ID ', ritis_response_jobID, ' was submitted and started successfully with UUID ', curr_uuid)
else:
    print('ERROR, RITIS Response Status Code for POST job submission request is ', ritis_status_code, ', printing entire POST Response')
    print(ritis_response.json())


############################################################################
    ##
    ## Second step: Check on the status of the job. This is a GET request.
    ## Stay in this step until the POST job finishes
    ##
    ########################################################################

job_status_state = job_status_UNDEFINED
while job_status_state == job_status_UNDEFINED:
    # Sleep a little    
    time.sleep(SECONDS_TO_SLEEP)
    print('Slept for ', SECONDS_TO_SLEEP, ' seconds')
    # Generate RITIS URL for the jobs status GET request
    job_status_url = "http://pda-api.ritis.org:8080/jobs/status?key=" + RITIS_API_KEY + "&jobId=" + ritis_response_jobID
    # Send the GET request
    job_status = requests.get(job_status_url)
    job_status_state = job_status.json()['state']
    # print('Now job status state is ', job_status_state, ' progressed to ', job_status.json()['progress'], '%')
job_status_code = job_status.status_code 
if job_status_code == status_code_OK: 
    job_status_json = job_status.json()
    job_status_state = job_status_json['state']
    job_status_progress = job_status_json['progress']
    print('Job status is ', job_status_state, ' succesfully progressed to ',  job_status_progress, '%')
else:
    print('ERROR, RITIS Response Status Code for job status request is ', job_status_code, ', printing entire JOB STATUS Response')
    print(job_status.json())


###########################################################################
    ##
    ## Third step: Export and download the results of the POST request.
    ## Here we know that the POST request finished
    ## This is a GET request.
    ##
    #######################################################################
    
# Generate RITIS URL for the export results GET request
results_export_url = "http://pda-api.ritis.org:8080/results/export?key=" + RITIS_API_KEY + "&uuid=" + curr_uuid
# Send the GET request
results_export = requests.get(results_export_url)
results_export_code = results_export.status_code
if results_export_code == status_code_OK:
    print('File successfully exported on RITIS side of things, transferring locally with curl...')
    outZIPfile = outZIPfilename + startDate + outZIPextension
    curl_cmd = "curl -X GET \"http://pda-api.ritis.org:8080/jobs/export/results?key=" + RITIS_API_KEY + "&uuid=" + curr_uuid + "\" -H \"accept: */*\"  --output " + outZIPfile 
    os.system(curl_cmd)
    print('Successfully downloaded the data locally')
    move_cmd = "move " + outZIPfile + " " + outZIPfolder
    os.system(move_cmd)
    os.chdir(outZIPfolder)
    unzip_cmd = "unzip -o " + outZIPfile
    os.system(unzip_cmd)
    print('Unzipped data locally')
    os.chdir('..')
else:
    print('ERROR, RITIS Response Status Code for Results Export request is ', results_export_code, ' printing entire RESULTS EXPORT STATUS RESPONSE')


###########################################################################
    ##
    ## Final step: If this run was successful, update the last_run file in 
    ## preparation for next run. If not, do not update last_run file.
    ##
    #######################################################################

if results_export_code == status_code_OK:
    last_run_file = open(last_run_filename, 'w')
    last_run_file.write(endDate + '\n' )
    last_run_file.close()

print('DONE!!\n\n')

