from internetarchive import get_item, download, get_session, upload
import os
from huggingface_hub import HfApi, login, delete_repo, create_repo, list_repo_files
from tqdm import tqdm
import os, time, shutil

# Authenticate
login(token="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")

api = HfApi()

access_key = 'sBksuaYmUmrMxunL'
secret_key = 'jEE4kD4Q5FeD9AxX'

def exist_in_collection1(identifier, filename):
    session = get_session(config={'s3': {'access': access_key, 'secret': secret_key}})
    item = session.get_item(identifier)
    for file in item.files:
        if filename in file['name']:
            return True
    return False

def exist_in_collection(repo_id, filename):
    try:
        files = list_repo_files(repo_id=repo_id, token='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', repo_type="dataset")
        print(f"Found {len(files)} files in the repository.")
        for file in files:
            if filename in file['name']:
                return True
        return False
    except Exception as e:
        print(f"Error listing files: {e}")
    return False

def transfer_files(identifier_from, identifier_to, metadata):
    session = get_session(config={'s3': {'access': access_key, 'secret': secret_key}})
    item = session.get_item(identifier_from)

    for file in item.files:
        if not exist_in_collection('luistiagos/rgh', file['name']):
            item.download(files=[file['name']], destdir='./', verbose=True)
            print("Download completed successfully!")
            try:
                #upload_file(identifier_to, metadata, f"./{identifier_from}/{file['name']}")
                upload_hf("luistiagos/rgh", f"./{identifier_from}/{file['name']}")
            except Exception as e:
                print(f"Upload {file['name']} failed.")
        
        
def upload_file_archive(identifier, metadata, filename):
    session = get_session(config={'s3': {'access': access_key, 'secret': secret_key}})
    files_to_upload = [filename]  # Replace with actual file paths
    item = session.get_item(identifier)
    request_kwargs = {'timeout': None}
    if item.metadata:
        print("Item exists. Adding files to the existing item.")
        response = item.upload(files=files_to_upload, metadata=metadata, access_key=access_key, secret_key=secret_key, 
                               verbose=True, request_kwargs=request_kwargs, delete=True)
    else:
        response = upload(identifier, files=files_to_upload, metadata=metadata, access_key=access_key, secret_key=secret_key, 
                          verbose=True, request_kwargs=request_kwargs, delete=True)
    
    if response[0].status_code == 200:
        print("Operation successful!")
        return True
    
    print("Operation failed:", response[0].status_code, response[0].content)
    return False


def upload_hf(repo_id, file_path):
    """Uploads a single file to Hugging Face repository with progress."""
    file_size = os.path.getsize(file_path)
    
    with tqdm(total=file_size, unit="B", unit_scale=True, desc=f"Uploading {os.path.basename(file_path)}", ascii=True) as pbar:
        future = api.upload_file(
            path_or_fileobj=file_path,
            path_in_repo=os.path.basename(file_path),
            repo_id=repo_id,
            repo_type="dataset",  # Ensure this is correct
            run_as_future=True
        )

        while not future.done():
            time.sleep(5)  # Poll every 5 seconds; adjust as needed

        # Finalize the progress bar on completion
        try:
            future.result()
            pbar.n = file_size
            pbar.refresh()
            print(f"{os.path.basename(file_path)} uploaded successfully!")
        except Exception as e:
            print(f"Error uploading {file_path}: {e}")
    

metadata = {
    'title': 'Switch Games'
}

def uploadfilesinfolder():
    folder_path = 'C:\projetos\InternetArchive\mx360gcpt2-x360-ztm'
    files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

    for file in files:
        try:
            print(f"Upload {file}")
            upload_file_archive('switchgames', metadata, f"./mx360gcpt2-x360-ztm/{file}")
        except:
            print('Operation failed on upload')
    
#upload_file('xbox360rghgames', metadata, './mx360gcpt2-x360-ztm/CSI.Deadly.Intent.USA.X360-ZTM.rar')

#uploadfilesinfolder()
transfer_files('mx360gcpt2-x360-ztm', 'xbox360rghgames', metadata)

#upload_file('switchgames', metadata, f"D:\switch\Splatoon 3 [NSP].rar")