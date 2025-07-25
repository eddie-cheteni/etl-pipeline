# -----------------------------------
# Do this to import needed libraries
# -----------------------------------
import numpy as np # type: ignore
import pandas as pd # type: ignore

import pysftp # type: ignore
import tempfile
import pyzipper
import shutil
import logging

import os
from pathlib import Path
import subprocess
from io import BytesIO

import paramiko
import getpass
import time

from datetime import date, timedelta, datetime
from typing import Tuple, Dict, List, Optional

import sys
from glob import glob

from tqdm.notebook import tqdm
from tqdm.version import __version__ as tqdm__version__

import warnings
warnings.simplefilter('ignore')

# ------------------
# Configure logging
# ------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_directories(base_path='data'):
    """Ensure all required directories exist"""
    data_path = Path(os.getenv('DATA_PATH', base_path))
    
    dirs = [
        data_path / 'raw',
        data_path / 'staging',
        data_path / 'processed',
        data_path / 'logs'
    ]
    
    for dir in dirs:
        dir.mkdir(parents=True, exist_ok=True)
    
    return data_path

# ------------------
# EXTRACT
# ------------------
def download_data():
    sftpHost = os.getenv('SFTP_HOST')
    sftpPort = os.getenv('PORT')
    uname = os.getenv('USER_NAME')
    pwd = os.getenv('PASSWORD')

    cnopts = pysftp.CnOpts()
    current_date = datetime.now().strftime('%Y%m%d')
    cnopts.hostkeys = None

    with pysftp.Connection(host=sftpHost, port=sftpPort, username=uname, password=pwd, cnopts=cnopts) as sftp:
        print('Connected to SFTP Server!!!')
        
        # Delete all existing Vilbev-*.zip files in raw directory
        for filename in os.listdir('./data/raw'):
            if filename.startswith('Vilbev-') and filename.endswith('.zip'):
                try:
                    os.remove(filename)
                    print(f'Deleted existing file: {filename}')
                except Exception as e:
                    print(f'Error deleting {filename}: {e}')

        # Download remote file with current date
        remote_file = f"/home/viljoenbev/Vilbev-{current_date}.zip"
        raw_file = f"./data/raw/Vilbev-{current_date}.zip"
        
        sftp.get(
            remotepath=remote_file,
            rawpath=raw_file, 
            preserve_mtime=True
        )
        print(f'Download is Complete!!! File saved as {raw_file}')
        
        
        
def extract_data():
    
    # Find the most recent ZIP file
    zip_file_path = os.path.join('./data/raw', glob('Vilbev-*.zip')[-1])
    
    # Open the ZIP file
    with pyzipper.AESZipFile(zip_file_path, 'r') as zip_ref:
        # List all files in the ZIP archive
        file_list = zip_ref.namelist()
        print("Files in the ZIP archive:", file_list)

        # Find the CSV file in the ZIP archive (assuming there's only one CSV file)
        csv_file_name = next((file for file in file_list if file.endswith('.csv')), None)
        
        if csv_file_name:
            print(f"Found CSV file: {csv_file_name}")
            
            # Extract the CSV file to a temporary location (optional)
            zip_ref.extract(csv_file_name, path="./data/raw")
            
            # Read the CSV file directly from the ZIP archive
            with zip_ref.open(csv_file_name) as csv_file:
                df = pd.read_csv(csv_file)
        else:
            print("No CSV file found in the ZIP archive.")
        
        
    df['Date']=pd.to_datetime(df['Date'], dayfirst=True, format="%d/%m/%Y")

    print("Removing all zero quantity sold.")
    df=df[df['Quantity']!=0] # REMOVES ALL ZERO
    
    print(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
    
    df.to_csv('./data/staging/viljoenbev.csv', index=False)

    return None

# ------------------
# TRANSFORM
# ------------------
def transform_data():
    """
    Function to transform Viljoen Beverages data
    
    Args:
        df: Input dataframe to transform
        returns: Transformed dataframe
        
    """
    df = os.path.join('./data/staging', 'viljoenbev.csv')
    
    lists=['SellerID','GUID','Date','Reference','Customer_Code','Name','Physical_Address1','Physical_Address2','Physical_Address3','Physical_Address4','Telephone',\
        'Stock_Code','Description','Price_Ex_Vat','Quantity','RepCode','ProductBarCodeID']
    df1=pd.DataFrame(columns=lists)

    # Create the new dataframe
    df1['Date']=df['Date']
    df1['SellerID']='VILJOEN'
    df1['GUID']=0
    df1['Reference']=df['Reference']
    df1['Customer_Code']=df['Customer code']
    df1['Name']=df['Customer name']
    df1['Physical_Address1']=df['Physical_Address1']
    df1['Physical_Address2']=df['Physical_Address2']
    df1['Physical_Address3']=df['Physical_Address3']
    df1['Physical_Address4']=(df['Deliver1']).fillna('').astype(str) +' '+(df['Deliver2']).fillna('').astype(str) +' '+(df['Deliver3']).fillna('').astype(str) +' '+(df['Deliver4']).fillna('').astype(str)
    df1['Telephone']=df['Telephone']
    df1['Stock_Code']=df['Product code']
    df1['Description']=df['Product description']
    df1['Price_Ex_Vat']=round(abs(df['Value']/df['Quantity']),2)
    df1['Quantity']=df['Quantity']
    df1['RepCode']=df['Rep']
    df1['ProductBarCodeID']=''
    
    # Fill missing values
    df1['Name'].fillna('SPAR NORTH RAND (11691)', inplace=True)
    
    # Calculate total quantity
    print(f"Total quantity: {np.sum(df1['Quantity']):.0f}")
    
    # Convert date to YYYY-MM-DD
    df2=df1.copy()
    df2['Date']=pd.to_datetime(df2['Date'])
    df2['Date']=df2['Date'].apply(lambda x: x.strftime("%Y-%m-%d"))
    
    # Remove all zero quantity sold
    df2=df2[df2['Quantity']!=0] # REMOVES ALL ZERO
    
    return df2


def save_data():
    """Saves cleaned data to CSV after deleting existing CSV files in the directory"""
    
    logger = get_run_logger()
    
    # Read the data
    df = pd.read_csv(os.path.join('./data/staging', 'viljoenbev.csv'))
    
    # Set the output directory
    processed_path = os.path.join('./data/processed')
    
    # Delete all CSV files in the processed data directory before saving new one
    csv_files = glob(os.path.join('./data/processed', '*.csv'))
    for file in csv_files:
        try:
            os.remove(file)
            logger.info(f"Deleted existing file: {file}")
        except OSError as e:
            logger.error(f"Error deleting file {file}: {e.strerror}")
    
    # Prepare the data
    data = df.copy()
    data['Date'] = pd.to_datetime(data['Date'])
    data['Date'] = data['Date'].apply(lambda x: x.strftime("%Y-%m-%d"))
    
    # Create filename
    filename = 'Viljoenbev_' + data['Date'].min() + '_to_' + data['Date'].max() + '.csv'
    
    # Save the new file to the processed data directory
    full_path = os.path.join(processed_path, filename)
    df.to_csv(full_path, index=False)
    
    # Logger
    logger.info(f"Data saved to {full_path}")
    
    return None

# -----------------------
# DATA VALIDATION
# -----------------------

def validate_data():
    """
    Validates data quality, checking for required fields
    
    NB: Do not forget to check manually - Date of transactions, compare Qty with original file
    """
    logger = get_run_logger()
    
    df = os.path.join('./data/processed', glob('Viljoenbev_*.csv')[-1])
    
    validation_errors = {
        "missing_quantity": df[df["Quantity"].isna()].index.tolist(),
        "missing_price": df[df["Price_Ex_Vat"].isna()].index.tolist(),
        "missing_description": df[df["Description"].isna() | (df["Description"] == "")].index.tolist(),
        "missing_customer_code": df[df["Customer_Code"].isna() | (df["Customer_Code"] == "")].index.tolist(),
        "missing_customer_name": df[df["Name"].isna() | (df["Name"] == "")].index.tolist(),
        "missing_stock_code": df[df["Stock_Code"].isna() | (df["Stock_Code"] == "")].index.tolist(),
        "missing_seller_id": df[df["SellerID"].isna() | (df["SellerID"] == "")].index.tolist(),
        "missing_date": df[df['Date'].isna() | (df['Date'] == '')].index.tolist()
    }
    
    has_errors = any(len(errors) > 0 for errors in validation_errors.values())
    
    if has_errors:
        for error_type, indices in validation_errors.items():
            if indices:
                logger.error(f"Validation error: {error_type} - {len(indices)} records affected")
    else:
        logger.info(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
        logger.info("Data validation passed")
    
    return not has_errors, validation_errors 
# ------------------
# LOAD
# ------------------
def upload_data_to_server():
    """
    Upload a specific CSV file to SFTP server.
        
    Returns:
    --------
    str or None
        Remote path where file was uploaded or None if failed
    """
    # Logger
    logger = get_run_logger()
    
    # Set SFTP connection parameters
    sftp_host = os.getenv('SFTP_HOST')
    sftp_port = os.getenv('PORT')
    sftp_user = os.getenv('USER_NAME')
    sftp_pass = os.getenv('PASSWORD')
    
    # Set the CSV file path
    csv_file_path = os.path.join('./data/processed', glob('Viljoenbev_*.csv')[0])
    remote_dir = '/home/viljoenbev/data/'
    
    try:
        # Verify the local file exists
        if not os.path.exists(csv_file_path):
            logger.error(f"Local file not found: {csv_file_path}")
            return None
            
        filename = os.path.basename(csv_file_path)
        remote_path = os.path.join(remote_dir, filename).replace('\\', '/')
        
        # Setup SFTP connection options
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None  # Disable host key checking (use with caution in production)
        
        # Connect and upload
        with pysftp.Connection(
            host=sftp_host, 
            port=sftp_port, 
            username=sftp_user, 
            password=sftp_pass, 
            cnopts=cnopts
        ) as sftp:
            logger.info(f"Connected to SFTP server: {sftp_host}")
            
            # Check if remote directory exists
            if not sftp.exists(remote_dir):
                logger.info(f"Remote directory {remote_dir} doesn't exist. Creating it...")
                sftp.makedirs(remote_dir)
                
            # Upload the file
            logger.info(f"Uploading {filename} to {remote_path}...")
            sftp.put(csv_file_path, remotepath=remote_path, confirm=True)
            logger.info("Upload completed successfully!")
            
            return remote_path
            
    except Exception as e:
        logger.error(f"Error uploading file: {str(e)}")
        return None
# ------------------
# RUN IMPORT SCRIPT
# ------------------
def run_import_script(timeout: int=120):
    """
    Connect to a remote server via SSH and execute a command.
    
    Args:
        hostname (str): The server hostname or IP address
        port (int): The SSH port number
        username (str): SSH username
        password (str, optional): SSH password. If None, will prompt for password
        command (str): The command to execute on the remote server
        timeout (int): Timeout for command execution in seconds
        
    Returns:
        tuple: (stdout, stderr, exit_code)
    """
    # Logger
    logger = get_run_logger()
    
    # SSH connection details
    hostname = os.getenv('HOST_NAME')
    port = os.getenv('PORT')
    username = os.getenv('FTP_USERNAME')
    password = os.getenv('FTP_PASSWORD')
    command = '/usr/local/eroute2market/supply_chain/scripts/importtxns.pl /home/viljoenbev/data 1'
    
    # If no password provided, prompt for it securely
    if password is None:
        password = getpass.getpass(f"Enter SSH password for {username}@{hostname}: ")
    
    # Create an SSH client instance
    client = paramiko.SSHClient()
    
    try:
        # Automatically add the server's host key
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Connect to the remote server
        logger.info(f"Connecting to {hostname} on port {port}...")
        client.connect(
            hostname=hostname, 
            port=port, 
            username=username, 
            password=password
            )
        
        logger.info(f"Connected. \nExecuting command:>>>>>>>> {command}")
        
        # Execute the command
        stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
        
        # Wait for the command to complete
        exit_status = stdout.channel.recv_exit_status()
        
        # Get the output
        stdout_str = stdout.read().decode('utf-8')
        stderr_str = stderr.read().decode('utf-8')
        
        # Print status
        if exit_status == 0:
            logger.info("Command executed successfully.")
        else:
            logger.error(f"Command failed with exit code: {exit_status}")
        
        # Return results
        return stdout_str, stderr_str, exit_status
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return "", str(e), -1
    
    finally:
        # Always close the connection
        client.close()
        logger.info("SSH connection closed.")
        
# ---------------------------
# SEND EMAIL
# ---------------------------
def send_email():
    pythoncom.CoInitialize()
    outlook = win32.Dispatch('Outlook.Application')
    mail = outlook.CreateItem(0)
    mail.To = ["eddwin@eroute2market.co.za","eddychetz@gmail.com"]
    mail.Subject = "Test Email from ETL process"
    mail.Body = "This is a test email sent using Outlook and Python"
    mail.Attachments.Add()
    mail.Display()
    # mail.Send()