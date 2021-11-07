import sqlite3
import hashlib
import os
import logging
from typing import NewType
import uuid
import boto3
import tempfile
from pathlib import Path
import easyocr
from pdf2image.generators import uuid_generator
import pdfplumber
from pdf2image import convert_from_path
from botocore.exceptions import ClientError
from dotenv import load_dotenv
load_dotenv()

reader = easyocr.Reader(['bs', 'hr','rs_latin'],gpu=False)

bucket = os.environ.get("BUCKET")


session = boto3.Session(
    aws_access_key_id=os.environ.get("ACCESS_KEY"),
    aws_secret_access_key=os.environ.get("SECRET_KEY"),
)

s3 = session.resource('s3')
s3_client = session.client('s3')


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

def get_all_files():
    with con:
        cur.execute("SELECT * FROM files WHERE converted = 0")
        return cur.fetchall()

def update_file_converted(hash):
    with con:
        cur.execute("""UPDATE files SET converted = 1
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

def upload_file(file_path, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_path: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_path is used
    :return: True if file was uploaded, else False
    """
    f = file_path.replace(os.sep, '/')
   
    # If S3 object_name was not specified, use file_path

    opath = os.path.split(object_name)[0]
    otxt = Path(object_name).stem + '.txt'
    object_name = os.path.join(opath,otxt)
    o = object_name.replace(os.sep, '/')

    
    # Upload the file
    try:
        response = s3_client.upload_file(f, bucket, o)
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
    f = folder_name.replace(os.sep, '/')
    f = f + '/'
    
    #Add folder
    try:
        response = s3_client.put_object(Bucket=bucket,Key=(f))
    except ClientError as e:
        logging.error(e)
        return False
    return True
    
    
        
    

def add_folders_to_s3():
    folders = get_all_folders()
    for i in range(len(folders)):
        if add_folder_to_s3(folders[i][1],bucket):
            update_folder_added(folders[i][0])
            

def add_files_to_s3():
    files = get_all_files()
    for i in range(2):
        if convert_using_PdfPlumber(files[i][1]):
            update_file_converted(files[i][0])
        else:
            if convert_using_EasyOCR(files[i][1]):
                update_file_converted(files[i][0])


def convert_using_PdfPlumber(pdf):
    print("Extracting text using pdfplubmer")

    with tempfile.TemporaryDirectory() as path:
        with pdfplumber.open(pdf) as pdfp:
            file_name = Path(pdf).stem + '.txt'
            file_path = os.path.join(path,file_name)
            
            for i in range(len(pdfp.pages)):
                page = pdfp.pages[i]
                text = page.extract_text()
                if text is None:
                    return False
                
                with open(file_path, "a", encoding="utf-8") as file_object:
                    file_object.write(text)
            
            return upload_file(file_path,bucket,pdf)
    
        
def convert_using_EasyOCR(pdf):
    print("Extracting text using EasyOCR")
    text_array = []
    with tempfile.TemporaryDirectory() as path:
        images_from_path = convert_from_path(pdf,500,output_folder=path)
        for image_counter,image in enumerate(images_from_path):
            image_name = "page_"+str(image_counter)+".jpg"
            image_path = os.path.join(path,image_name)
            image.save(image_path,'JPEG')
            result = reader.readtext(image_path,detail = 0, paragraph=True)
            text_array.extend(result)
        text_string = " ".join(text_array)
        file_name = Path(pdf).stem + '.txt'
       
        file_path = os.path.join(path,file_name)
        with open(file_path, 'w',encoding="utf-8") as f:
            f.write(text_string)
        return upload_file(file_path,bucket,pdf)
    
def main():
    
    create_tables()
    traverse_dirs()
    add_folders_to_s3()
    add_files_to_s3()

    
    con.close()

if __name__ == "__main__":
    main()










