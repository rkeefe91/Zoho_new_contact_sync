#!/usr/bin/env python
# coding: utf-8

# In[96]:


import os
from datetime import date, datetime
import pandas as pd
import numpy as np
import pyodbc    
import pytz
import logging
tz = pytz.timezone('America/Chicago')

# Set up enhanced logging
def setup_detailed_logger():
    logger = logging.getLogger('zoho_sync')
    logger.setLevel(logging.INFO)
    
    # Ensure config directory exists (important for Docker containers)
    config_dir = './config'
    if not os.path.exists(config_dir):
        os.makedirs(config_dir, exist_ok=True)
    
    # Create file handler for detailed logs
    log_file_path = os.path.join(config_dir, 'zoho_sync_errors.log')
    file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # Create console handler for immediate feedback (important for Docker logs)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Avoid duplicate handlers if logger already exists
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger

# Initialize the logger
detailed_logger = setup_detailed_logger()



# In[97]:


server = 'relitix-prod.database.windows.net'
database = 'ravel'
username = 'PythonUser'
password = '6T8hrDS3#^)'
driver='{ODBC Driver 17 for SQL Server}'

db = pyodbc.connect(
  driver=driver,
  server=server,
  database=database,
  uid=username,
  pwd=password
)

server_params = {
    'driver': driver,
    'server': server,
    'database': database,
    'username': username,
    'password': password
}


# In[98]:


import zcrmsdk

#https://github.com/zoho/zohocrm-python-sdk


# In[99]:




from zcrmsdk.src.com.zoho.crm.api import HeaderMap, ParameterMap
from zcrmsdk.src.com.zoho.crm.api.attachments import Attachment
from zcrmsdk.src.com.zoho.crm.api.layouts import Layout
from zcrmsdk.src.com.zoho.crm.api.record import *
from zcrmsdk.src.com.zoho.crm.api.record import Record as ZCRMRecord
from zcrmsdk.src.com.zoho.crm.api.tags import Tag
from zcrmsdk.src.com.zoho.crm.api.users import User
from zcrmsdk.src.com.zoho.crm.api.util import Choice, StreamWrapper
from zcrmsdk.src.com.zoho.crm.api.fields import LookupField
from zcrmsdk.src.com.zoho.crm.api.user_signature import UserSignature
from zcrmsdk.src.com.zoho.crm.api.dc import USDataCenter
from zcrmsdk.src.com.zoho.api.authenticator.store import DBStore, FileStore
from zcrmsdk.src.com.zoho.api.logger import Logger
from zcrmsdk.src.com.zoho.crm.api.initializer import Initializer
from zcrmsdk.src.com.zoho.api.authenticator.oauth_token import OAuthToken, TokenType
from zcrmsdk.src.com.zoho.crm.api.sdk_config import SDKConfig


# In[100]:


class SDKInitializer(object):

    @staticmethod
    def initialize():

        base_path = os.getenv('ZOHO_INTEGRATION_PATH', './Zoho integration/config')
        log_file_path = './config/python_sdk_log.log'
        tokens_file_path = './config/python_sdk_tokens.txt'
        resource_path = './config'

        logger = Logger.get_instance(level=Logger.Levels.INFO, file_path=log_file_path)

        # Create an UserSignature instance that takes user Email as parameter
        user = UserSignature(email='rob@relitix.com')
        environment = USDataCenter.PRODUCTION()
        client_id = '1000.KV1CH0AK7KIJJNDEEY3H854VL2JIUJ'
        client_secret = 'ced8904126819ff6dd7f5350d541140d59293e071c'
        grant_token = "1000.5b04554ebd5340b8ff719a4d611868a4.1fa66c8c4f057f737f14a6a48a95ed7d"      
        redirect_url = 'https://www.relitix.com'

        token = OAuthToken(client_id=client_id, client_secret=client_secret,                           token=grant_token, token_type=TokenType.GRANT,                           redirect_url=redirect_url)

        store = FileStore(file_path=tokens_file_path)

        config = SDKConfig(auto_refresh_fields=True, pick_list_validation=False)

        Initializer.initialize(user=user, environment=environment, token=token, store=store, sdk_config=config, resource_path=resource_path, logger=logger)



#SDKInitializer.initialize()


# In[101]:


import datetime as dt
import re

def clean_phone_number(phone_str):
    """
    Clean phone number by removing hidden control characters and invalid characters.
    Returns a properly formatted phone number or None if invalid.
    """
    if phone_str is None or pd.isna(phone_str):
        return None
    
    # Convert to string if not already
    phone_str = str(phone_str)
    
    # Log the original value for debugging
    detailed_logger.debug(f"Original phone value: {repr(phone_str)}")
    
    # Remove all control characters (ASCII 0-31 and 127)
    # This includes hidden characters like \x00, \x01, etc.
    cleaned = ''.join(char for char in phone_str if ord(char) >= 32 and ord(char) != 127)
    
    # Remove any remaining non-printable characters
    cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)
    
    # Remove extra whitespace
    cleaned = cleaned.strip()
    
    # If empty after cleaning, return None
    if not cleaned or cleaned.lower() in ['null', 'none', 'n/a', '']:
        return None
    
    # Remove any non-digit, non-space, non-dash, non-parentheses, non-plus characters
    # Keep only valid phone number characters
    cleaned = re.sub(r'[^\d\s\-\(\)\+\.]', '', cleaned)
    
    # Remove excessive spaces and normalize
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    # If still empty or too short, return None
    if not cleaned or len(re.sub(r'[^\d]', '', cleaned)) < 7:
        return None
    
    detailed_logger.debug(f"Cleaned phone value: {repr(cleaned)}")
    return cleaned

def clean_text_field(text_str):
    """
    Clean text fields by removing control characters while preserving valid content.
    """
    if text_str is None or pd.isna(text_str):
        return None
    
    # Convert to string if not already
    text_str = str(text_str)
    
    # Remove control characters but keep printable ones
    cleaned = ''.join(char for char in text_str if ord(char) >= 32 and ord(char) != 127)
    
    # Remove any remaining non-printable characters
    cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)
    
    # Clean up whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    return cleaned if cleaned else None

def get_latest_id(ids_with_dates):
    # Convert date strings to datetime objects
    ids_with_dates = [(i, dt.datetime.strptime(d, '%Y-%m-%d %H:%M:%S%z')) for i,d in ids_with_dates]
    
    # Find the tuple with the most recent datetime
    latest_tuple = max(ids_with_dates, key=lambda x: x[1])
    
    # Return the id from the latest tuple
    return latest_tuple[0]


# In[102]:


def get_records_by_email(target_email, verbose = False):


    ## Retrieve records with email = test_email from CRM


    # Get instance of RecordOperations Class
    record_operations = RecordOperations()


    # Get instance of ParameterMap Class
    param_instance = ParameterMap()


    param_instance.add(SearchRecordsParam.email, target_email)


    # Get instance of BodyWrapper Class that will contain the request body
    request = BodyWrapper()
    # Set the list to data in BodyWrapper instance
    #request.set_data(records_list)

    #duplicate_check_fields = ['Mail_Merge_Name']

    # Set the array containing duplicate check fields to BodyWrapper instance
    #request.set_duplicate_check_fields(duplicate_check_fields)

    # Get instance of HeaderMap Class
    header_instance = HeaderMap()

    # header_instance.add(UpsertRecordsHeader.x_external, "Leads.External")


    # Call the get_records method with the module name and params
    response = record_operations.search_records('Contacts', param_instance, header_instance)


    if response is not None:
        # Get the status code from response
        if verbose:
            print('Status Code: ' + str(response.get_status_code()))

        # Get object from response
        response_object = response.get_object()

        if response_object is not None:

            # Check if expected ActionWrapper instance is received.
            if isinstance(response_object, ActionWrapper):

                # Get the list of obtained ActionResponse instances
                action_response_list = response_object.get_data()

                for action_response in action_response_list:

                    # Check if the request is successful
                    if isinstance(action_response, SuccessResponse):
                        # Get the Status
                        if verbose:
                            print("Status: " + action_response.get_status().get_value())

                        # Get the Code
                        if verbose:
                            print("Code: " + action_response.get_code().get_value())

                        if verbose:
                            print("Details")

                        # Get the details dict
                        details = action_response.get_details()

                        for key, value in details.items():
                            
                            if verbose:
                                print(key + ' : ' + str(value))

                        # Get the Message
                        if verbose:
                            print("Message: " + action_response.get_message().get_value())

                    # Check if the request returned an exception
                    elif isinstance(action_response, APIException):
                        # Get the Status
                        if verbose:
                            print("Status: " + action_response.get_status().get_value())

                        # Get the Code
                        if verbose:
                            print("Code: " + action_response.get_code().get_value())

                        if verbose:
                            print("Details")

                        # Get the details dict
                        details = action_response.get_details()

                        for key, value in details.items():
                            if verbose:
                                print(key + ' : ' + str(value))

                        # Get the Message
                        if verbose:
                            print("Message: " + action_response.get_message().get_value())

            # Check if the request returned an exception
            elif isinstance(response_object, APIException):
                # Get the Status
                print("Status: " + response_object.get_status().get_value())

                # Get the Code
                print("Code: " + response_object.get_code().get_value())

                print("Details")

                # Get the details dict
                details = response_object.get_details()

                for key, value in details.items():
                    print(key + ' : ' + str(value))

                # Get the Message
                print("Message: " + response_object.get_message().get_value())

    response_object = response.get_object()
    
    if response_object is not None:
        
        results_list = []
    
        record_list = response_object.get_data()

        for record in record_list:
                        # Get the ID of each Record
            
            if verbose:
                print("Record ID: " + str(record.get_id())) 

                # Get the createdBy User instance of each Record
            created_by = record.get_created_by()
            if created_by is not None:
                # Get the Name of the created_by User
                if verbose:
                    print("Record Created By - Name: " + created_by.get_name())

                # Get the ID of the created_by User
                if verbose:
                    print("Record Created By - ID: " + str(created_by.get_id()))

                # Get the Email of the created_by User
                if verbose:
                    print("Record Created By - Email: " + created_by.get_email())

            # Get the CreatedTime of each Record
            if verbose:
                print("Record CreatedTime: " + str(record.get_created_time()))

            if record.get_modified_time() is not None:
            # Get the ModifiedTime of each Record
                if verbose:
                    print("Record ModifiedTime: " + str(record.get_modified_time()))
                
            
            results_list.append([str(record.get_id()), str(record.get_created_time())])



            if verbose:
                print("Record Field Value: " + str(record.get_key_value('Acct_created_in_portal')))
            
        return get_latest_id(results_list)
        
    else:
        if verbose:
            print('No record found')
        
        return None

        


# In[103]:


def create_new_record(df,i,account_zid,verbose = True):

    contact_zid = None

    # Get instance of RecordOperations Class
    record_operations = RecordOperations()

    # Get instance of BodyWrapper Class that will contain the request body
    request = BodyWrapper()

    # List to hold Record instances
    records_list = []

    # Get instance of Record Class
    record = ZCRMRecord()

    # Log the attempt with user details
    user_email = df.UserEmail.iloc[i]
    account_name = df.AccountName.iloc[i]
    detailed_logger.info(f"Attempting to create contact for user: {user_email}, Account: {account_name}, Account_ZID: {account_zid}")

    # Clean and validate data before adding to record
    cleaned_mobile = clean_phone_number(df.UserCellPhone.iloc[i])
    cleaned_linkedin = clean_text_field(df.LinkedInId.iloc[i])
    cleaned_email = clean_text_field(df.UserEmail.iloc[i])
    cleaned_first_name = clean_text_field(df.FirstName.iloc[i])
    cleaned_last_name = clean_text_field(df.LastName.iloc[i])
    
    # Log data cleaning results
    if cleaned_mobile != df.UserCellPhone.iloc[i]:
        detailed_logger.info(f"Cleaned mobile for {user_email}: '{df.UserCellPhone.iloc[i]}' -> '{cleaned_mobile}'")
    
    record.add_field_value(Field.Contacts.last_name(), cleaned_last_name)
    record.add_field_value(Field.Contacts.first_name(), cleaned_first_name)
    record.add_field_value(Field('Email'), cleaned_email)
    
    # Only add mobile if it's valid after cleaning
    if cleaned_mobile:
        record.add_field_value(Field('Mobile'), cleaned_mobile)
        detailed_logger.debug(f"Added mobile for {user_email}: {cleaned_mobile}")
    else:
        detailed_logger.warning(f"Skipping invalid mobile for {user_email}: {repr(df.UserCellPhone.iloc[i])}")
    
    # Only add LinkedIn if it's valid after cleaning
    if cleaned_linkedin:
        record.add_field_value(Field('LinkedIn'), cleaned_linkedin)
    
    record.add_field_value(Field('Acct_created_in_portal'), tz.localize(df.CreatedOn.iloc[i])) 
    
    record_field = Record()
    record_field.set_id(account_zid)
    record.add_field_value(Field('Account_Name'), record_field)    

    # Add Record instance to the list
    records_list.append(record)

    # Set the list to data in BodyWrapper instance
    request.set_data(records_list)

    header_instance = HeaderMap()

    # Call create_records method that takes BodyWrapper instance and module_api_name as parameters
    response = record_operations.create_records('Contacts', request)

    if response is not None:
        # Get the status code from response
        detailed_logger.info(f"Response status code for {user_email}: {response.get_status_code()}")
        if verbose:
            print('Status Code: ' + str(response.get_status_code()))

        # Get object from response
        response_object = response.get_object()

        if response_object is not None:

            # Check if expected ActionWrapper instance is received.
            if isinstance(response_object, ActionWrapper):

                # Get the list of obtained ActionResponse instances
                action_response_list = response_object.get_data()

                for action_response in action_response_list:

                    # Check if the request is successful
                    if isinstance(action_response, SuccessResponse):
                        # Get the Status
                        detailed_logger.info(f"SUCCESS - Contact created for {user_email}: Status: {action_response.get_status().get_value()}")
                        if verbose:
                            print("Status: " + action_response.get_status().get_value())

                        # Get the Code
                        if verbose:
                            print("Code: " + action_response.get_code().get_value())

                        if verbose:
                            print("Details")
                            print(df.UserEmail.iloc[i] )

                        # Get the details dict
                        details = action_response.get_details()

                        for key, value in details.items():
                            if verbose:
                                print(key + ' : ' + str(value))
                            
                            if key == 'id':
                                contact_zid = value
                                detailed_logger.info(f"Contact ID created for {user_email}: {contact_zid}")

                        # Get the Message
                        if verbose:
                            print("Message: " + action_response.get_message().get_value())

                    # Check if the request returned an exception
                    elif isinstance(action_response, APIException):
                        # Enhanced error logging with context
                        error_status = action_response.get_status().get_value()
                        error_code = action_response.get_code().get_value()
                        error_message = action_response.get_message().get_value()
                        
                        detailed_logger.error(f"API_EXCEPTION - Failed to create contact for {user_email}")
                        detailed_logger.error(f"  Account: {account_name} (ZID: {account_zid})")
                        detailed_logger.error(f"  Status: {error_status}")
                        detailed_logger.error(f"  Code: {error_code}")
                        detailed_logger.error(f"  Message: {error_message}")
                        
                        # Get the Status
                        print("Status: " + error_status)

                        # Get the Code
                        print("Code: " + error_code)

                        print("Details")

                        # Get the details dict
                        details = action_response.get_details()

                        for key, value in details.items():
                            detailed_logger.error(f"  Detail - {key}: {value}")
                            print(key + ' : ' + str(value))

                        # Get the Message
                        print("Message: " + error_message)

            # Check if the request returned an exception
            elif isinstance(response_object, APIException):
                # Enhanced error logging for response-level exceptions
                error_status = response_object.get_status().get_value()
                error_code = response_object.get_code().get_value()
                error_message = response_object.get_message().get_value()
                
                detailed_logger.error(f"RESPONSE_EXCEPTION - Failed to create contact for {user_email}")
                detailed_logger.error(f"  Account: {account_name} (ZID: {account_zid})")
                detailed_logger.error(f"  Status: {error_status}")
                detailed_logger.error(f"  Code: {error_code}")
                detailed_logger.error(f"  Message: {error_message}")
                
                # Get the Status
                print("Status: " + error_status)

                # Get the Code
                print("Code: " + error_code)

                print("Details")

                # Get the details dict
                details = response_object.get_details()

                for key, value in details.items():
                    detailed_logger.error(f"  Detail - {key}: {value}")
                    print(key + ' : ' + str(value))

                # Get the Message
                print("Message: " + error_message)
                
    return contact_zid


# In[104]:


def update_contact_record(df,i,contact_zid, verbose = True):

        # Get instance of RecordOperations Class
        record_operations = RecordOperations()

        # Get instance of BodyWrapper Class that will contain the request body
        request = BodyWrapper()

        # List to hold Record instances
        records_list = []

        # Get instance of Record Class
        record1 = ZCRMRecord()
        
        # ID of the record to be updated
        record1.set_id(contact_zid)
        
        record1.add_field_value(Field('Acct_created_in_portal'), tz.localize(df.CreatedOn.iloc[i]) ) 
        
        # Add Record instance to the list
        records_list.append(record1)
        
        
        request.set_data(records_list)

        header_instance = HeaderMap()

        # Call update_records method that takes BodyWrapper instance and module_api_name as parameter.
        response = record_operations.update_records('Contacts', request, header_instance)

        if response is not None:
            # Get the status code from response
            if verbose:
                print('Status Code: ' + str(response.get_status_code()))

            # Get object from response
            response_object = response.get_object()

            if response_object is not None:

                # Check if expected ActionWrapper instance is received.
                if isinstance(response_object, ActionWrapper):

                    # Get the list of obtained ActionResponse instances
                    action_response_list = response_object.get_data()

                    for action_response in action_response_list:

                        # Check if the request is successful
                        if isinstance(action_response, SuccessResponse):
                            # Get the Status
                            if verbose:
                                print("Status: " + action_response.get_status().get_value())

                            # Get the Code
                            if verbose:
                                print("Code: " + action_response.get_code().get_value())

                            if verbose:
                                print("Details")

                            # Get the details dict
                            details = action_response.get_details()

                            for key, value in details.items():
                                if verbose:
                                    print(key + ' : ' + str(value))

                            # Get the Message
                            if verbose:
                                print("Message: " + action_response.get_message().get_value())

                        # Check if the request returned an exception
                        elif isinstance(action_response, APIException):
                            # Get the Status
                            print("Status: " + action_response.get_status().get_value())

                            # Get the Code
                            print("Code: " + action_response.get_code().get_value())

                            print("Details")

                            # Get the details dict
                            details = action_response.get_details()

                            for key, value in details.items():
                                print(key + ' : ' + str(value))

                            # Get the Message
                            print("Message: " + action_response.get_message().get_value())

                # Check if the request returned an exception
                elif isinstance(response_object, APIException):
                    # Get the Status
                    print("Status: " + response_object.get_status().get_value())

                    # Get the Code
                    print("Code: " + response_object.get_code().get_value())

                    print("Details")

                    # Get the details dict
                    details = response_object.get_details()

                    for key, value in details.items():
                        print(key + ' : ' + str(value))

                    # Get the Message
                    print("Message: " + response_object.get_message().get_value())
 
        
    
    


# In[105]:


from datetime import datetime
import pandas as pd

def extract_date_from_datetime(datetime_input):
    """
    Extracts the date portion from a datetime string or a Pandas Timestamp.
    
    Parameters:
        datetime_input (str or pd.Timestamp): The datetime string in 'YYYY-MM-DD HH:MM:SS.ssssss' format
                                              or a Pandas Timestamp object
    
    Returns:
        date: A Python date object
    """
    try:
        # Check if input is a Pandas Timestamp and convert it to string
        if isinstance(datetime_input, pd.Timestamp):
            datetime_str = datetime_input.strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            datetime_str = datetime_input
        
        # Convert the datetime string to a datetime object
        datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
        
        # Extract the date
        date_obj = datetime_obj.date()
        
        return date_obj
    
    except ValueError as e:
        print(f"An error occurred: {e}")
        return None



# In[106]:


def update_contact_record_last_login(df,i,contact_zid, verbose = False):

        # Get instance of RecordOperations Class
        record_operations = RecordOperations()

        # Get instance of BodyWrapper Class that will contain the request body
        request = BodyWrapper()

        # List to hold Record instances
        records_list = []

        # Get instance of Record Class
        record1 = ZCRMRecord()
        
        # ID of the record to be updated
        record1.set_id(contact_zid)
        

        record1.add_field_value(Field('Last_login_date'), extract_date_from_datetime(df.most_recent_login.iloc[i]) ) 
        
        # Add Record instance to the list
        records_list.append(record1)
        
        
        request.set_data(records_list)

        header_instance = HeaderMap()

        # Call update_records method that takes BodyWrapper instance and module_api_name as parameter.
        response = record_operations.update_records('Contacts', request, header_instance)

        if response is not None:
            # Get the status code from response
            if verbose:
                print('Status Code: ' + str(response.get_status_code()))

            # Get object from response
            response_object = response.get_object()

            if response_object is not None:

                # Check if expected ActionWrapper instance is received.
                if isinstance(response_object, ActionWrapper):

                    # Get the list of obtained ActionResponse instances
                    action_response_list = response_object.get_data()

                    for action_response in action_response_list:

                        # Check if the request is successful
                        if isinstance(action_response, SuccessResponse):
                            # Get the Status
                            if verbose:
                                print("Status: " + action_response.get_status().get_value())

                            # Get the Code
                            if verbose:
                                print("Code: " + action_response.get_code().get_value())

                            if verbose:
                                print("Details")

                            # Get the details dict
                            details = action_response.get_details()

                            for key, value in details.items():
                                if verbose:
                                    print(key + ' : ' + str(value))

                            # Get the Message
                            if verbose:
                                print("Message: " + action_response.get_message().get_value())

                        # Check if the request returned an exception
                        elif isinstance(action_response, APIException):
                            # Get the Status
                            print("Status: " + action_response.get_status().get_value())

                            # Get the Code
                            print("Code: " + action_response.get_code().get_value())

                            print("Details")

                            # Get the details dict
                            details = action_response.get_details()

                            for key, value in details.items():
                                print(key + ' : ' + str(value))

                            # Get the Message
                            print("Message: " + action_response.get_message().get_value())

                # Check if the request returned an exception
                elif isinstance(response_object, APIException):
                    # Get the Status
                    print("Status: " + response_object.get_status().get_value())

                    # Get the Code
                    print("Code: " + response_object.get_code().get_value())

                    print("Details")

                    # Get the details dict
                    details = response_object.get_details()

                    for key, value in details.items():
                        print(key + ' : ' + str(value))

                    # Get the Message
                    print("Message: " + response_object.get_message().get_value())
 
        
    
    


# In[107]:


def get_account_id_by_name(target_account_name, verbose=False):


    ## Retrieve records with email = test_email from CRM


    # Get instance of RecordOperations Class
    record_operations = RecordOperations()


    # Get instance of ParameterMap Class
    param_instance = ParameterMap()


    param_instance.add(SearchRecordsParam.word,  target_account_name)
#    param_instance.add(GetRecordParam.fields, 'Account_Name');







    # Get instance of BodyWrapper Class that will contain the request body
    request = BodyWrapper()
    # Set the list to data in BodyWrapper instance
    #request.set_data(records_list)

    #duplicate_check_fields = ['Mail_Merge_Name']

    # Set the array containing duplicate check fields to BodyWrapper instance
    #request.set_duplicate_check_fields(duplicate_check_fields)

    # Get instance of HeaderMap Class
    header_instance = HeaderMap()

    # header_instance.add(UpsertRecordsHeader.x_external, "Leads.External")


    # Call the get_records method with the module name and params
    response = record_operations.search_records('Accounts', param_instance, header_instance)



    if response is not None:
        # Get the status code from response
        if verbose:
            print('Status Code: ' + str(response.get_status_code()))

        # Get object from response
        response_object = response.get_object()

        if response_object is not None:

            # Check if expected ActionWrapper instance is received.
            if isinstance(response_object, ActionWrapper):

                # Get the list of obtained ActionResponse instances
                action_response_list = response_object.get_data()

                for action_response in action_response_list:

                    # Check if the request is successful
                    if isinstance(action_response, SuccessResponse):
                        # Get the Status
                        if verbose:
                            print("Status: " + action_response.get_status().get_value())

                        # Get the Code
                        if verbose:
                            print("Code: " + action_response.get_code().get_value())

                        if verbose:
                            print("Details")

                        # Get the details dict
                        details = action_response.get_details()

                        for key, value in details.items():
                            if verbose:
                                print(key + ' : ' + str(value))

                        # Get the Message
                        if verbose:
                            print("Message: " + action_response.get_message().get_value())

                    # Check if the request returned an exception
                    elif isinstance(action_response, APIException):
                        # Get the Status
                        print("Status: " + action_response.get_status().get_value())

                        # Get the Code
                        print("Code: " + action_response.get_code().get_value())

                        print("Details")

                        # Get the details dict
                        details = action_response.get_details()

                        for key, value in details.items():
                            print(key + ' : ' + str(value))

                        # Get the Message
                        print("Message: " + action_response.get_message().get_value())

            # Check if the request returned an exception
            elif isinstance(response_object, APIException):
                # Get the Status
                print("Status: " + response_object.get_status().get_value())

                # Get the Code
                print("Code: " + response_object.get_code().get_value())

                print("Details")

                # Get the details dict
                details = response_object.get_details()

                for key, value in details.items():
                    print(key + ' : ' + str(value))

                # Get the Message
                print("Message: " + response_object.get_message().get_value())

    response_object = response.get_object()
    
    if response_object is not None:
        
        results_list = []
    
        record_list = response_object.get_data()

        for record in record_list:
                        # Get the ID of each Record
            if verbose:
                print("Record ID: " + str(record.get_id())) 

                # Get the createdBy User instance of each Record
            created_by = record.get_created_by()
            if created_by is not None:
                # Get the Name of the created_by User
                if verbose:
                    print("Record Created By - Name: " + created_by.get_name())

                # Get the ID of the created_by User
                if verbose:
                    print("Record Created By - ID: " + str(created_by.get_id()))

                # Get the Email of the created_by User
                if verbose:
                    print("Record Created By - Email: " + created_by.get_email())

            # Get the CreatedTime of each Record
            if verbose:
                print("Record CreatedTime: " + str(record.get_created_time()))

            if record.get_modified_time() is not None:
            # Get the ModifiedTime of each Record
                if verbose:
                    print("Record ModifiedTime: " + str(record.get_modified_time()))
                
            
            results_list.append([str(record.get_id())])



            if verbose:
                print("Record Field Value: " + str(record.get_key_value('Acct_created_in_portal')))
            
        return results_list
        
    else:
        if verbose:
            print('No record found')
        
        return None

        


# In[108]:


def get_account_data(zid, verbose = False):
    
        # Get instance of RecordOperations Class
        record_operations = RecordOperations()

        # Get instance of ParameterMap Class
        param_instance = ParameterMap()

        

        fields = ['Account_Name', 'Primary_MLS']

        for field in fields:
            param_instance.add(GetRecordParam.fields, field)
            
                # Get instance of HeaderMap Class
        header_instance = HeaderMap()
        
                # Call getRecord method that takes param_instance, header_instance, module_api_name and record_id as parameter
        response = record_operations.get_record(zid,'Accounts', param_instance, header_instance) 
        
        
        if response is not None:

            # Get the status code from response
            if verbose:
                print('Status Code: ' + str(response.get_status_code()))

            if response.get_status_code() in [204, 304]:
                print('No Content' if response.get_status_code() == 204 else 'Not Modified')
                return

            # Get object from response
            response_object = response.get_object()

            if response_object is not None:

                # Check if expected ResponseWrapper instance is received.
                if isinstance(response_object, ResponseWrapper):

                    # Get the list of obtained Record instances
                    record_list = response_object.get_data()

                    for record in record_list:
                        # Get the ID of each Record

                        if verbose:
                            print("Record ID: " + str(record.get_id())) 
                        

                        # Get the createdBy User instance of each Record
                        created_by = record.get_created_by()

                        # Check if created_by is not None
                        if created_by is not None:
                            # Get the Name of the created_by User
                            if verbose:
                                print("Record Created By - Name: " + created_by.get_name())

                            # Get the ID of the created_by User
                            if verbose:
                                print("Record Created By - ID: " + created_by.get_id())

                            # Get the Email of the created_by User
                            if verbose:
                                print("Record Created By - Email: " + created_by.get_email())

                        # Get the CreatedTime of each Record
                        if verbose:
                            print("Record CreatedTime: " + str(record.get_created_time()))

                        if record.get_modified_time() is not None:
                            # Get the ModifiedTime of each Record
                            if verbose:
                                print("Record ModifiedTime: " + str(record.get_modified_time()))

                        # Get the modified_by User instance of each Record
                        modified_by = record.get_modified_by()

                        # Check if modified_by is not None
                        if modified_by is not None:
                            # Get the Name of the modified_by User
                            if verbose:
                                print("Record Modified By - Name: " + modified_by.get_name())

                            # Get the ID of the modified_by User
                            if verbose:
                                print("Record Modified By - ID: " + modified_by.get_id())

                            # Get the Email of the modified_by User
                            if verbose:
                                print("Record Modified By - Email: " + modified_by.get_email())


                        # To get particular field value
                        if verbose:
                            print("Record Field Value: " + str(record.get_key_value('Account_Name')))
                        if verbose:
                            print("Record Field Value: " + str(record.get_key_value('Primary_MLS')))

                        
                        return(str(record.get_key_value('Account_Name')),record.get_key_value('Primary_MLS'),zid)


# In[109]:


def find_data(data_list, account_name):
    matching_data = []
    for data in data_list:
        if data[0] == account_name:
            matching_data.append(data)
    if len(matching_data) > 0:
        if len(matching_data) == 1:
            return matching_data[0]
        else:
            for data in matching_data:
                if data[1] == True:
                    return data
            # If no tuple with second element True, return first matching tuple
            return matching_data[0]
    else:
        return None


# In[110]:


def get_zoho_acct_id(df_accts, RelitixAccountId):
    """
    This function takes a dataframe and a RelitixAccountId as input, and
    returns the corresponding zoho_acct_id from the same row if the
    RelitixAccountId is present in the dataframe, otherwise returns None.
    """
    if RelitixAccountId in df_accts['RelitixAccountId'].values:
        return df_accts.loc[df_accts['RelitixAccountId'] == RelitixAccountId, 'zoho_acct_id'].iloc[0]
    else:
        return None


# In[111]:


def locate_account_zid(account_name,RelitixAccountId,df_accts):
    
    
    #Check to see if account in table

    acct_id_from_table = get_zoho_acct_id(df_accts, RelitixAccountId)
    
    if acct_id_from_table is None:
    
        # Get list of account ids with fuzzy match on account name
        account_ids = get_account_id_by_name(account_name)

        if account_ids is None:
            print('Account not found')
            return None

        # Convert to integer list
        account_ids_int = []
        for sublist in account_ids:
            for elem in sublist:
                account_ids_int.append(int(elem))


        account_output = []

        # Assemble metadata on accounts
        for zid in account_ids_int:
            account_output.append(get_account_data(zid))

        account_zid = find_data(account_output, account_name)

        if account_zid is None:
            print('No primary MLS account set')
            return None
        else:        
            return account_zid[2]
    
    else:
        return int(acct_id_from_table)


# In[112]:


def store_zid_in_table(df,i,contact_zid):
    
    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+password)
    
    cursor = cnxn.cursor()
    
    user_email = df.UserEmail.iloc[i]
    contact_zid = int(contact_zid)
    
    
    # Define the query to upsert the data
    query = f"""
        MERGE portal.users_zoho_ids AS target
        USING (VALUES ('{user_email}', {contact_zid})) AS source (UserEmail, zoho_id)
        ON (target.UserEmail = source.UserEmail)
        WHEN MATCHED THEN
            UPDATE SET target.zoho_id = source.zoho_id
        WHEN NOT MATCHED THEN
            INSERT (UserEmail, zoho_id)
            VALUES (source.UserEmail, source.zoho_id);
        """
    cursor.execute(query)
    # Commit the changes to the database
    cnxn.commit()

    # Close the cursor and database connections
    cursor.close()
    cnxn.close()    


# In[113]:


def fetch_data_from_server(table_name, server_params):
    try:
        connection_string = 'DRIVER={};SERVER={};PORT=1433;DATABASE={};UID={};PWD={}'.format(
            server_params['driver'],
            server_params['server'],
            server_params['database'],
            server_params['username'],
            server_params['password']
        )
        
        cnxn = pyodbc.connect(connection_string)
        
        query = f"select * from {table_name}"
        df = pd.read_sql(query, cnxn)
        
        return df
    except Exception as e:
        print(f"Error occurred: {e}")
        return None
    finally:
        if 'cnxn' in locals() or 'cnxn' in globals():
            cnxn.close()


# START CODE HERE- Add new users

# In[114]:


df = fetch_data_from_server('portal.py_portal_users', server_params)


# In[115]:


df_acct = fetch_data_from_server('portal.accounts_zoho_ids', server_params)


# In[116]:


SDKInitializer.initialize()


# In[117]:


created_contacts = 0
updated_contacts = 0

# Log the start of processing
detailed_logger.info(f"Starting contact sync process for {df.shape[0]} users")

for i in range(df.shape[0]):
    
    user_email = df.UserEmail.iloc[i]
    account_name = df.AccountName.iloc[i]
    relitix_account_id = df.RelitixAccountId.iloc[i]
    
    detailed_logger.info(f"Processing user {i+1}/{df.shape[0]}: {user_email}")
    
    # Check to see if email is in zoho
    match_zid = get_records_by_email(df.UserEmail.iloc[i])
    
    if match_zid == None:
        
        #New user to zoho - add a new record
        detailed_logger.info(f"User {user_email} not found in Zoho, creating new contact")
        
        account_zid = locate_account_zid(df.AccountName.iloc[i], df.RelitixAccountId.iloc[i],df_acct)
        if account_zid is None:
            error_msg = f'No account exists in Zoho: {account_name} (Relitix ID: {relitix_account_id})'
            detailed_logger.error(f"ACCOUNT_NOT_FOUND - {error_msg} for user {user_email}")
            print(error_msg)
            print('User not added')
            continue

        contact_zid = None    
        contact_zid = create_new_record(df,i,account_zid)
        if contact_zid == None:
            detailed_logger.error(f"CONTACT_CREATION_FAILED - No contact created for {user_email}")
            print('No contact created')
            continue
        else:
            store_zid_in_table(df,i,contact_zid)
            created_contacts +=1
            detailed_logger.info(f"Successfully created contact for {user_email} with ID {contact_zid}")
        
    else:
        match_zid = int(match_zid)
        detailed_logger.info(f"User {user_email} found in Zoho with ID {match_zid}, updating existing contact")
        update_contact_record(df,i,match_zid)
        store_zid_in_table(df,i,match_zid)
        updated_contacts +=1
        
        
        
# Log final summary
detailed_logger.info(f"Contact sync process completed:")
detailed_logger.info(f"  Created contacts: {created_contacts}")
detailed_logger.info(f"  Updated contacts: {updated_contacts}")
detailed_logger.info(f"  Total processed: {created_contacts + updated_contacts}")

print("Created contacts: " + str(created_contacts))
print("Updated contacts: " + str(updated_contacts)) 
    
    
    


# Update last login date

# In[118]:


df_li = fetch_data_from_server('portal.last_3_days_logins', server_params)


# In[119]:


# Initialize the progress bar
total_rows = len(df_li)
print("Progress: ", end="")

# Iterate through each row and call update_contact_record
for index, (df_index, row) in enumerate(df_li.iterrows()):
    update_contact_record_last_login(df_li, df_index, row['zoho_id'], verbose=False)
    
    # Update progress bar
    completed = (index + 1) / total_rows * 100
    print(f"\rProgress: [{int(completed // 10) * '#'}{(10 - int(completed // 10)) * ' '}] {completed:.2f}%", end="")
    
print("\nCompleted!")


#
# In[120]:
print("Current working directory:", os.getcwd())
# %%
