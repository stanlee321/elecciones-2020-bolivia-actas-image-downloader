import pandas as pd
import requests
import shutil

from tqdm import tqdm


    
def download_data(id:int):
    if len(id) == 1:
        id = "2500" + str(id)
    if len(id) == 2:
        id = "250" + str(id)
    if len(id) == 3:
        id = "25" + str(id)

    # This is the image url.
    image_url = f"https://s3.amazonaws.com/archivo.computo/actas/{id}.jpg"
    print(image_url)
    # Open the url image, set stream to True, this will return the stream content.
    resp = requests.get(image_url, stream=True)
    # Open a local file with wb ( write binary ) permission.
    local_file = open(f'{id}.jpg', 'wb')
    # Set decode_content value to True, otherwise the downloaded image file's size will be zero.
    resp.raw.decode_content = True
    # Copy the response stream raw data to local image file.
    shutil.copyfileobj(resp.raw, local_file)
    # Remove the image url response object.
    del resp


def main(file_path:str):

    
    df = pd.read_excel(file_path, sheet_name=0)


    print(df.columns)

    ids = list(set(df["ID_RECINTO"].to_list()))

    for _id in tqdm(ids):
        download_data(str(_id))

if __name__ == '__main__':

    file_path = "exportacion_EG2020_20201021_170800_1552309146707785949.xlsx"
    main(file_path)