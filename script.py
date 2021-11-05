import sqlite3
import hashlib
import os
import logging
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
load_dotenv()

bucket = os.environ.get("bucket")

con = sqlite3.connect('data.db')
cur = con.cursor()

def create_tables():
    with con:
        cur.execute('''
                    CREATE TABLE IF NOT EXISTS files(
                        hash TEXT PRIMARY KEY, 
                        path TEXT, 
                        converted INTEGER)
        ''')
    
        cur.execute('''
                    CREATE TABLE IF NOT EXISTS folders(
                        hash TEXT PRIMARY KEY, 
                        path TEXT, 
                        added INTEGER)
        ''')
   

def add_file(hash,path,converted=0):
    
    with con:
        cur.execute("INSERT INTO files VALUES (?, ?, ?)", (hash, path,converted))

def add_folder(hash,path,added=0):
    with con:
        cur.execute("INSERT INTO folders VALUES (?, ?, ?)", (hash, path,added))
        
def get_file(hash):
    with con:
        cur.execute(""" SELECT * FROM files WHERE hash = :hash""", {'hash':hash})
        return cur.fetchone()
    
def get_folder(hash):
    with con:
        cur.execute(""" SELECT * FROM folders WHERE hash = :hash""", {'hash':hash})
        return cur.fetchone()

def get_all_folders():
    with con:
        cur.execute("SELECT * FROM folders WHERE added = 0")
        return cur.fetchall()

def update_folder_added(hash):
    with con:
        cur.execute("""UPDATE folders SET added = 1
                    WHERE hash = :hash""",
                  {'hash': hash })
    
    
        

def traverse_dirs():
    path = os.walk(".")
    for root, directories, files in path:
        for directory in directories:
            if root.lower().find("git") == -1 and directory.lower().find("git") == -1 :
                d = os.path.join(root,directory)
                dhash = hash_path(d)
                is_added = get_folder(dhash)
                if is_added is None:
                    add_folder(dhash,d)
                
                
        for file in files:
            if file.lower().endswith(('.pdf')):
                f = os.path.join(root,file)
                fhash = hash_path(f)
                is_converted = get_file(fhash)
                if is_converted is None:
                    add_file(fhash,f)
            
                
def hash_path(path):
    hash_object = hashlib.md5(path.encode())
    return hash_object.hexdigest()

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def add_folder_to_s3(folder_name, bucket):
    """Add folder to an S3 bucket

    :param folder_name: Folder to add
    :param bucket: Bucket to add folder to
    :return: True if file was uploaded, else False
    """

    # Add folder
    s3_client = boto3.client('s3')
    try:
        response = s3_client.put_object(Bucket=bucket,Key=(folder_name+'/'))
    except ClientError as e:
        logging.error(e)
        return False
    return True
        
    
    
def add_folders_to_s3():
    folders = get_all_folders()
    for i in range(len(folders)):
        add_folder_to_s3(folders[i],bucket)

def add_files_to_s3():
    return

def main():
    
    create_tables()
    traverse_dirs()
    add_folders_to_s3()
    
    
    con.close()

if __name__ == "__main__":
    main()
