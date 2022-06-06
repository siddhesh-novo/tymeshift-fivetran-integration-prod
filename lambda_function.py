import requests
import datetime
import sys, os
import logging
import warnings, time
import random
warnings.simplefilter(action='ignore', category=FutureWarning)
import datetime
import json
import calendar
from pympler import asizeof
import pickle
###########################################  Python Logger Configuration ###########################

class InfoFilter(logging.Filter):
    """
        FIlter Class for Only Capturing
        Warning and Error level logs.
    """

    def filter(self, rec):
        return rec.levelno in (logging.INFO, logging.ERROR, logging.WARNING)


def logger_config():
    """
        Python loger config to filter out log level.
    """
    logging.basicConfig(format='%(asctime)s  %(levelname)-s  %(name)s %(message)s', datefmt="%d-%m-%Y %H:%M:%S",
                        handlers=[logging.StreamHandler(sys.stdout)])

    logger = logging.getLogger(__name__)
    logger.addFilter(InfoFilter())
    logging.getLogger().setLevel(logging.INFO)
    warnings.simplefilter(action='ignore', category=FutureWarning)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    
def validate_response(response):
    """
    This function is helper function to validate_response
    response.

    :param response(string) response from api epoint

    """
    if int(response.status_code) != 200:
        logging.error("Something has failed, please see below error message.")
        logging.error(response.content)
        logging.error(response.status_code)
        logging.error(response.url)
        sys.exit(1)


def generate_timestamp(last_refresh_ts, timestamp_interval, max_day_one_time):
    converted_d1 = datetime.datetime.utcfromtimestamp(last_refresh_ts) # UTC
    new_refresh_date_dt = datetime.datetime.utcnow()
    new_refresh_date_ts = int(calendar.timegm(new_refresh_date_dt.utctimetuple()))
    this_run_track_ts = new_refresh_date_ts
    new_refresh_date_ts_track = new_refresh_date_ts
    timestamp_list = []
    
    has_more = True
    if ((new_refresh_date_ts_track-last_refresh_ts)/(60*60*24)) > timestamp_interval:
        while has_more:
            if ((new_refresh_date_ts_track-last_refresh_ts)/(60*60*24)) <  max_day_one_time:
                logging.info(f"Differnce between last refresh time and current time is less then maximum days can be processed at once. {((new_refresh_date_ts_track-last_refresh_ts)/(60*60*24))} < {max_day_one_time}")
                if ((new_refresh_date_ts_track-last_refresh_ts)/(60*60*24)) > timestamp_interval:
                    last_refresh_ts = last_refresh_ts
                    new_refresh_date_ts = int(calendar.timegm((datetime.datetime.utcfromtimestamp(last_refresh_ts) + datetime.timedelta(timestamp_interval)).utctimetuple()))
                    timestamp_list.append(last_refresh_ts)
                    timestamp_list.append(new_refresh_date_ts)
                    last_refresh_ts = new_refresh_date_ts
                elif ((new_refresh_date_ts_track-last_refresh_ts)/(60*60*24)) == timestamp_interval or ((new_refresh_date_ts_track-last_refresh_ts)/(60*60*24)) < timestamp_interval:
                    logging.info(f"Reached to the last limit of current time.")
                    last_refresh_ts = new_refresh_date_ts
                    new_refresh_date_ts = new_refresh_date_ts_track
                    timestamp_list.append(last_refresh_ts)
                    timestamp_list.append(new_refresh_date_ts)
                    has_more = False
                    break
            else:
                if has_more is False:
                    logging.info("Already Processed - Differnce between last refresh time and current time is less then maximum days. Stopping the loop now.") 
                    break
                else:
                    logging.info(f"Differnce between last refresh time and current time is greater then maximum days can be processed at once. {((new_refresh_date_ts_track-last_refresh_ts)/(60*60*24))} > {max_day_one_time}")
                    new_refresh_date_ts = min(int(calendar.timegm(new_refresh_date_dt.utctimetuple())), int(calendar.timegm((datetime.datetime.utcfromtimestamp(last_refresh_ts) + datetime.timedelta(max_day_one_time)).utctimetuple())))
                    new_refresh_date_ts_track = new_refresh_date_ts
                    logging.info(f"Setting new refresh time to adding {max_day_one_time} in last refresh time. {last_refresh_ts} {new_refresh_date_ts}")

                    while True:
                        if ((new_refresh_date_ts_track-last_refresh_ts)/(60*60*24)) > timestamp_interval:
                            last_refresh_ts = last_refresh_ts
                            new_refresh_date_ts = int(calendar.timegm((datetime.datetime.utcfromtimestamp(last_refresh_ts) + datetime.timedelta(timestamp_interval)).utctimetuple()))
                            timestamp_list.append(last_refresh_ts)
                            timestamp_list.append(new_refresh_date_ts)
                            last_refresh_ts = new_refresh_date_ts
                        elif ((new_refresh_date_ts_track-last_refresh_ts)/(60*60*24)) == timestamp_interval or ((new_refresh_date_ts_track-last_refresh_ts)/(60*60*24)) < timestamp_interval:
                            new_refresh_date_ts = new_refresh_date_ts_track
                            timestamp_list.append(last_refresh_ts)
                            timestamp_list.append(new_refresh_date_ts)
                            logging.info(timestamp_list)
                            break
                    has_more = False
    else:
        timestamp_list.append(last_refresh_ts)
        timestamp_list.append(new_refresh_date_ts)

    timestamp_list = [timestamp_list[i:i+2] for i in range(0,len(timestamp_list),2)]
   
    return timestamp_list, timestamp_list[len(timestamp_list)-1][1], this_run_track_ts
    

def get_ticket(ticket_endpoint, access_key, ticket_last_refresh_time, per_call_limit, ticket_latest_offset):
    """
    :param tickets_endpoint:
    :param access_key:
    :return:
    """
    ticket_list_all = []
    new_returned_offset = 0
    total_retrieved = 0
    max_min_fetched_date = []
    is_all_fetched = False
    hasMoreticketRecord = "No"
    logging.info(f"Retrieving tickets.")
    headers = {"Accept": "application/json", "Authorization": f"AccessKey {random.choice(access_key)}"}
    limit_per_page = 20
    current_offset = ticket_latest_offset
    logging.info(f"Range of ticket from {current_offset} to {current_offset + limit_per_page} and has to go til the date {ticket_last_refresh_time} starting from current time.")
    params = {"limit": limit_per_page, "offset": current_offset}
    req_ticket = requests.get(ticket_endpoint, headers=headers, params=params)
    validate_response(req_ticket)
    req_ticket = req_ticket.json()
    
    if req_ticket['items'] is  None:
        logging.info("No records to fetch, returing empty handed.")
        is_all_fetched = True
    else:
        total_retrieved += 20
        current_offset += limit_per_page
        total_count = req_ticket['totalCount']

        for i in range(0, len(req_ticket['items'])):
            if req_ticket['items'][i]['createdAt'] is not None:
                dtime_obj = datetime.datetime.fromisoformat(req_ticket['items'][i]['createdAt'][0:19])
            else:
                dtime_obj = datetime.datetime.fromisoformat('2200-07-09T07:49:58')
            
            max_min_fetched_date.append(dtime_obj)
            if dtime_obj >= ticket_last_refresh_time:
                ticket_list_all.append(req_ticket['items'][i])
            else:
                logging.info(f"Got first less value {dtime_obj} then {ticket_last_refresh_time}, Stopping the parse.")
                new_returned_offset = 0
                is_all_fetched = True
                break


        while current_offset < total_count and is_all_fetched == False:
            if total_retrieved < per_call_limit:
                if current_offset + limit_per_page > total_count:
                    limit_per_page = total_count - current_offset
                    
                logging.info(f"Range of ticket from {current_offset} to {current_offset + limit_per_page} and has to go til the date {ticket_last_refresh_time} starting from current time.")
                headers = {"Accept": "application/json", "Authorization": f"AccessKey {random.choice(access_key)}"}
                params = {"limit": limit_per_page, "offset": current_offset}
                total_retrieved += 20
                req_ticket = requests.get(ticket_endpoint, headers=headers, params=params)
                validate_response(req_ticket)
                req_ticket = req_ticket.json()
                
                if req_ticket['items'] is not None:
                    current_offset += limit_per_page
                    for i in range(0, len(req_ticket['items'])):
                        if req_ticket['items'][i]['createdAt'] is not None:
                            dtime_obj = datetime.datetime.fromisoformat(req_ticket['items'][i]['createdAt'][0:19])
                        else:
                            dtime_obj = datetime.datetime.fromisoformat('2200-07-09T07:49:58')
                            
                        max_min_fetched_date.append(dtime_obj)
                        if dtime_obj >= ticket_last_refresh_time:
                            ticket_list_all.append(req_ticket['items'][i])
                        else:
                            logging.info(f"Got first less value {dtime_obj} then {ticket_last_refresh_time}, Stopping the parse.")
                            is_all_fetched = True
                            break   
            
                else:
                    is_all_fetched = True
            else:
                new_returned_offset = current_offset
                break
    
        limit_per_page = 20
        logging.info("Even though offset has reached the limitation, there might be more record. Great job messagebird!")
        limit_cross = True
        
        while limit_cross == True and is_all_fetched == False:
            if total_retrieved < per_call_limit:
                logging.info(f"Range of ticket from {current_offset} to {current_offset + limit_per_page} and has to go til the date {ticket_last_refresh_time} starting from current time.")
                headers = {"Accept": "application/json", "Authorization": f"AccessKey {random.choice(access_key)}"}
                params = {"limit": limit_per_page, "offset": current_offset}
                total_retrieved += 20
                req_ticket = requests.get(ticket_endpoint, headers=headers, params=params)
                validate_response(req_ticket)
                req_ticket = req_ticket.json()
                
                if req_ticket['items'] is not None:
                    current_offset += limit_per_page
                    for i in range(0, len(req_ticket['items'])):
                        if req_ticket['items'][i]['createdAt'] is not None:
                            dtime_obj = datetime.datetime.fromisoformat(req_ticket['items'][i]['createdAt'][0:19])
                        else:
                            dtime_obj = datetime.datetime.fromisoformat('2200-07-09T07:49:58')
                            
                        max_min_fetched_date.append(dtime_obj)
                        if dtime_obj >= ticket_last_refresh_time:
                            ticket_list_all.append(req_ticket['items'][i])
                        else:
                            logging.info(f"Got first less value {dtime_obj} then {ticket_last_refresh_time}, Stopping the parse.")
                            is_all_fetched = True
                            break
                else:
                    is_all_fetched = True
            else:
                new_returned_offset = current_offset
                break
        logging.info(f"ticket fetched from {max(max_min_fetched_date)} till {min(max_min_fetched_date)}. Goal is to parse till {ticket_last_refresh_time}")
        
    if is_all_fetched == False:
        hasMoreticketRecord = "Yes"
        logging.info(f"Processed {total_retrieved} and new offset will be {new_returned_offset} and has more record - {hasMoreticketRecord}.")
    else:
        logging.info(f"Processed {total_retrieved}. No more record for tickets.")
    
    return ticket_list_all, new_returned_offset, hasMoreticketRecord

    
def transorm_dic(data_dict):
    data_list = []
    for key, value in data_dict.items():
        d_dic = {"id" : key, "value" : value}
        data_list.append(d_dic)
        
    return data_list
    
    
def get_results(endpoint, param, call_type):
    if call_type == 'timesheet':
        timesheet_list = []
        response = requests.get(endpoint, params = param, timeout = None)
        validate_response(response)
        response = response.json()
        if response['result'] is not None:
            timesheet_list += response['result']['data'] 
        
            total_count = int(response['result']['total'])
            param["offset"] += param["limit"]
            
            while param["offset"] < total_count:
                if param["offset"] + param["limit"] > total_count:
                    param["limit"] = total_count - param["offset"] 
                # logging.info(f"\t\t\t\tRange for messages from {param['offset']} to {param['offset'] + param['limit']}.")
                response = requests.get(endpoint,params=param, timeout = None)
                validate_response(response)
                response = response.json()
                param["offset"] += param["limit"]
                if response['result'] is not None:
                    timesheet_list += response['result']['data']
                    
        return timesheet_list
        
    if call_type == 'basic_report':
        data_basicreport_list = []
        data_jobcode_list = []
        response = requests.get(endpoint, params = param, timeout = None)
        validate_response(response)
        response = response.json()
        if 'result_total' in response:
            data_jobcode_list += response['result_total']['data']['totals']['job_codes']
            if len(response['zend_tickets']) > 0:
                response = transorm_dic(response['zend_tickets'])
                for i in range(0, len(response)):
                    data = {}
                    data['zend_ticket_id'] = response[i]['id']
                    VALUE = response[i]['value']
                    for k,v in VALUE.items():
                        if k == 'id' or k == 'ID':
                            k = 'nice_id'
                        data[k] = v
                    response[i] = data
                
                data_basicreport_list += response
            
        return data_basicreport_list, data_jobcode_list
        
        
def get_users(config_dict):
    user_endpoint = "https://novo.tymeapp.com/api/listusers"
    logging.info("Fetching user details.")
    param = {"user" : config_dict["account_name"], "token" : config_dict["token"], "requester_email" : config_dict["requester_email"]}
    
    user_list_all =[]
    response = requests.get(user_endpoint, params = param, timeout = None)
    validate_response(response)
    response = response.json()
    user_list_all += response['result']
    
    return user_list_all

def get_groups(config_dict):
    group_endpoint = "https://novo.tymeapp.com/api/listgroups"
    logging.info("Fetching group details.")
    param = {"group" : config_dict["account_name"], "token" : config_dict["token"], "requester_email" : config_dict["requester_email"]}
    
    group_list_all = []
    response = requests.get(group_endpoint, params = param, timeout = None)
    validate_response(response)
    response = response.json()
    response = transorm_dic(response['result'])
    group_list_all += response

    return group_list_all


def get_timecategories(config_dict):
    timecategories_endpoint = "https://novo.tymeapp.com/api/listtimecategories"
    param = {"timecategories" : config_dict["account_name"], "token" : config_dict["token"], "requester_email" : config_dict["requester_email"]}
    
    timecategories_df = pd.DataFrame()
    response = requests.get(timecategories_endpoint, params = param, timeout = None)
    validate_response(response)
    response = response.json()
    timecategories_df = timecategories_df.append(pd.DataFrame(response['result']), ignore_index=True)
    timecategories_df = timecategories_df.fillna(np.nan)
    timecategories_df.columns = timecategories_df.columns.str.upper()
    timecategories_df.to_csv("D:/timecategories_df.csv", quotechar='"', quoting=csv.QUOTE_ALL, index=False, sep = ',')   


def get_basicreport(config_dict, timestamp_list):
    basic_report_list_all = []
    jobcodes_list_all = []
    basicreport_endpoint = f'https://novo.tymeapp.com/api/basicreport'
    logging.info("Fetching basicreport details.")
    
    for ts in timestamp_list:
        param = {"user" : config_dict["account_name"], "token" : config_dict["token"], "requester_email" : config_dict["requester_email"], "payroll_period_start" : 0, "start_time": ts[0] , "end_time" : ts[1] }
        logging.info(f"Requesting from {datetime.datetime.utcfromtimestamp(ts[0])} till {datetime.datetime.utcfromtimestamp(ts[1])}.")
        data_basicreport_list, data_jobcode_list = get_results(basicreport_endpoint, param, 'basic_report')
        
        for i in range(0, len(data_basicreport_list)):
            data_basicreport_list[i]['start_time'], data_basicreport_list[i]['end_time'] = ts[0], ts[1]
            
        for i in range(0, len(data_jobcode_list)):
            data_jobcode_list[i]['start_time'], data_jobcode_list[i]['end_time'] = ts[0], ts[1]
                   
        basic_report_list_all += data_basicreport_list
        jobcodes_list_all += data_jobcode_list
    
    return basic_report_list_all,jobcodes_list_all


def get_timesheet(config_dict, timestamp_list):
    timesheet_all_list = []
    timesheet_endpoint = f'https://novo.tymeapp.com/api/listtimesheets'
    logging.info("Fetching timesheet details.")
    for ts in timestamp_list:
        param = {"user" : config_dict["account_name"], "token" : config_dict["token"], "requester_email" : config_dict["requester_email"], "payroll_period_start" : 0, "start_time": ts[0] , "end_time" : ts[1] , "limit" : 100 , "offset" : 0}
        logging.info(f"Requesting from {datetime.datetime.utcfromtimestamp(ts[0])} till {datetime.datetime.utcfromtimestamp(ts[1])}.")
        timesheet_all_list += get_results(timesheet_endpoint, param, 'timesheet')
        
    return timesheet_all_list
    
    
def lambda_handler(request, context):
    logger_config()
    hasMoreRecord = False
    start_time = time.time()
    # 1427980698 1605633190 1605633190
    config_dict = {}
    config_dict["token"] = request['secrets']['token']
    config_dict["requester_email"] = request['secrets']['requester_email']
    config_dict["account_name"] = request['secrets']['account_name']
    
    if 'setup_test' in request:
        if request['setup_test'] == True:
            logging.info("This is the setup test, no data will be fetched.")
            return {}
            
    timestamp_interval = 0.1
    max_day_one_time = 0.1
    default_historical_state = {"last_refresh_date" : 1427980698, "is_offset_req" : "No", "last_refresh_datetime" : str(datetime.datetime.utcfromtimestamp(1427980698))}
    

    if len(request['state']) == 0:
        logging.info("This is historical sync invocation, lambda will perform historical data sync.")
        request['state'] = default_historical_state
        
    state = request['state']
    is_offset_req = state["is_offset_req"]
    logging.info(f"Recieved state is {request['state']}.")
    logging.info(f"Offset request status - {is_offset_req}") 
    timestamp_list, new_refresh_date_ts, has_more_timestamp = generate_timestamp(state["last_refresh_date"], timestamp_interval, max_day_one_time)
    logging.info(f"New Refresh timestamp will be {new_refresh_date_ts}. timestamp_list - {timestamp_list}")

    timesheet_all_list = get_timesheet(config_dict, timestamp_list)
    basic_report_list_all,jobcodes_list_all = get_basicreport(config_dict, timestamp_list)
    
    user_list_all, group_list_all = [], []
    if is_offset_req == 'No':
        logging.info(f"This is not offset request, so request will fetch user and group data")
        user_list_all = get_users(config_dict)
        group_list_all = get_groups(config_dict)
        
    if timestamp_list[len(timestamp_list)-1][1] != has_more_timestamp:
        logging.info(f"There are more record to fetch, will make a new api call.")
        hasMoreRecord = True
    
    print(len(timesheet_all_list), len(user_list_all), len(group_list_all), len(basic_report_list_all), len(jobcodes_list_all))
    insert = {
            "TIMESHEET": timesheet_all_list,
            "USER": user_list_all,
            "GROUP": group_list_all,
            "BASIC_REPORT": basic_report_list_all,
            "JOB_CODE": jobcodes_list_all
        }

        
    is_offset_req = "Yes" if hasMoreRecord is True else "No"
    state = {"last_refresh_date": new_refresh_date_ts, "is_offset_req" : is_offset_req, "last_refresh_datetime" : str(datetime.datetime.utcfromtimestamp(new_refresh_date_ts))}
    schema = {
        "TIMESHEET": {
            "primary_key": ["id"]
        },
        "USER": {
            "primary_key": ["id"]
        }, 
        "GROUP": {
            "primary_key": ["id"]
        },
        "BASIC_REPORT": {
            "primary_key": ["zend_ticket_id", "start_time", "end_time"]
        },
        "JOB_CODE": {
            "primary_key": ["zend_ticket_id", "start_time", "end_time"]
        }}
        
    response = {
        "state": state,
        "insert": insert,
        "schema" : schema,
        "hasMore": hasMoreRecord,
    }


    logging.info(f"Total Runtime For Execution is -> {round((time.time() - start_time)/60,2)} Minutes.")
    logging.info(f"Returned state is {state}.")
    logging.info(f"Size of the payload is - {(float(len(pickle.dumps(response))))/1048576} MB.")
    return response
    
    
