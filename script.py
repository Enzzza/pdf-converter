import sqlite3
import hashlib
import os
import boto3

s3 = boto3.client('s3')
bucket_name = "bucket-name-here"

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

def add_folders_to_s3():
    folders = get_all_folders()
    for i in range(len(folders)):
        #s3.put_object(Bucket=bucket_name, Key=(folders[i]+'/'))
        print(folders[i])

def add_files_to_s3():
    return

def main():
    create_tables()
    traverse_dirs()
    add_folders_to_s3()
    
    
    con.close()

if __name__ == "__main__":
    main()
