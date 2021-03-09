import pandas as pd
import requests
import shutil
from glob import glob
from tqdm import tqdm
import os
import shutil  


def download_pro(folder:str, id:str, image_url:str, ext:str):
    # Open the url image, set stream to True, this will return the stream content.
    resp = requests.get(image_url, stream=True)
    # Open a local file with wb ( write binary ) permission.
    local_file = open(f'{folder}/{id}.{ext}', 'wb')
    # Set decode_content value to True, otherwise the downloaded image file's size will be zero.
    resp.raw.decode_content = True
    # Copy the response stream raw data to local image file.
    shutil.copyfileobj(resp.raw, local_file)
    # Remove the image url response object.
    del resp

    
def download_data(id:int, dep_name:str, datetime:str):
    if len(id) == 1:
        id = "2500" + str(id)
    if len(id) == 2:
        id = "250" + str(id)
    if len(id) == 3:
        id = "25" + str(id)

    # This is the image url.
    #image_url = f"https://s3.amazonaws.com/archivo.computo/actas/{id}.jpg"

    image_url = f"https://s3.amazonaws.com/sn-archivo-computo/actas/{id}.jpg"
    image_url_png = f"https://s3.amazonaws.com/sn-archivo-computo/actas/{id}.png"

    download_pro(folder = f"images/{dep_name}/{datetime}/jpg", image_url = image_url, ext = "jpg", id = id)
    download_pro(folder = f"images/{dep_name}/{datetime}/png", image_url = image_url_png, ext = "png", id = id)

    print(image_url)


def main(file_path:str, ):

    
    df = pd.read_csv(file_path, sep="|", encoding='latin-1')

    splited = file_path.split("_")
    dep_name  = splited[1]
    datetime = splited[2]+ "_" + splited[3]

    os.makedirs(os.path.join("images", dep_name, datetime, "jpg" ), exist_ok=True) 
    os.makedirs(os.path.join("images", dep_name, datetime, "png" ), exist_ok=True) 

    ids = list(set(df["CODIGO_MESA"].to_list()))

    for _id in tqdm(ids):
        download_data(str(_id), dep_name= dep_name,  datetime=datetime )

if __name__ == '__main__':
    files = glob("queue/*.csv")
    for file in files:
        main(file)
        source = file
        destination = os.path.join("data", file.split("/")[-1])
        shutil.move(source, destination)
