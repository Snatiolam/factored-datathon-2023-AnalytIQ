from azure.storage.filedatalake import DataLakeServiceClient
import os

storage_account = "safactoreddatathon"
credential = "sp=rle&st=2023-07-25T18:12:36Z&se=2023-08-13T02:12:36Z&sv=2022-11-02&sr=c&sig=l2TCTwPWN8LSM922lR%2Fw78mZWQK2ErEOQDUaCJosIaw%3D"

service = DataLakeServiceClient(account_url="https://safactoreddatathon.dfs.core.windows.net/", 
                                credential="sp=rle&st=2023-07-25T18:12:36Z&se=2023-08-13T02:12:36Z&sv=2022-11-02&sr=c&sig=l2TCTwPWN8LSM922lR%2Fw78mZWQK2ErEOQDUaCJosIaw%3D")

file_system = service.get_file_system_client(file_system="source-files")

file_paths = file_system.get_paths()

def download_json_files(directory_client, local_base_directory):
    file_list = directory_client.get_paths()

    for file_or_directory in file_list:
        if not file_or_directory.is_directory:
            # Descargar solo los archivos con extensión .json.gz
            file_name = file_or_directory.name
            if file_name.endswith(".json.gz"):
                path = file_name.split('/')
                table = path[0]
                if table == 'amazon_reviews' or table == 'amazon_metadata':
                    if not os.path.exists(f"{local_base_directory}/{table}"):
                        os.makedirs(f"{local_base_directory}/{table}")
                    file = path[2]
                    print(table)
                    local_file_path = f"{local_base_directory}/{table}/{file}"
                    file_client = directory_client.get_file_client(file_name)
                    with open(local_file_path, "wb") as local_file:
                        downloader = file_client.download_file()
                        downloader.readinto(local_file)
                print("Archivo descargado exitosamente:", local_file_path)

download_json_files(file_system, local_base_directory="data")

file_system.close()

service.close()