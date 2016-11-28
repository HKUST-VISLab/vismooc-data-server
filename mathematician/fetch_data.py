from . import httphelper as http
import tarfile
import gzip
import os

class DownloadFileFromServer():
    """Download file from server"""
    def __init__(self, api_key, host="https://dataapi2.hkmooc.hk"):
        self.__api_key = api_key
        self.__token = None
        self.__host = host
        self.__http_connection = http.HttpConnection(self.__host)
    def get_token_from_server(self):
        """ Get the Access Token from server using API Key
        """
        self.__http_connection.headers = {"Authorization" : "Token " + self.__api_key}
        response = self.__http_connection.post("/resources/access_tokens", None)
        assert response.get_return_code() == 200
        response_json = response.get_content_json()
        self.__token = response_json.get("collection").get("items")[0].get("accessToken")
        return self.__token
    def get_click_stream(self, start, end, save_dir):
        """Get click stream from server, the parameters start
           and end should be unix timestamp in millisecond
        """
        if self.__token is None:
            self.get_token_from_server()
        self.__http_connection.headers = {"Authorization" : "Token " + self.__token}
        response = self.__http_connection.get("/resources/clickstreams", \
            {"since": str(start), "before": str(end)})
        assert response.get_return_code() == 200
        items = response.get_content_json().get('collection').get('items')
        file_urls = []
        file_names = []
        for item in items:
            file_urls.append(item['href'])
            file_names.append(item['href'][item['href'].rindex("/")+1:])
        self.__http_connection.download_files(file_urls, save_dir)
        file_paths = [os.path.join(save_dir, file_name) for file_name in file_names]
        self.decompress_files(file_paths, "gzip")

    def get_mongodb_and_mysqldb_snapshot(self, save_dir):
        """ Download mongodb and mysql snapshot
        """
        if self.__token is None:
            self.get_token_from_server()
        db_list = ["dbsnapshots_mongodb", "dbsnapshots_mysqldb"]
        self.__http_connection.headers = {"Authorization" : "Token " + self.__token}
        # self.__http_connection.download_files(file_urls, save_dir,)
        urls = [self.__host + "/resources/" + db_name for \
               db_name in db_list]
        self.__http_connection.download_files(urls, save_dir)
        # file_paths = [os.path.join(save_dir, file_name) for file_name in db_list]
        self.decompress_files([os.path.join(save_dir, "dbsnapshots_mongodb"),], "gtar")
        self.decompress_files([os.path.join(save_dir, "dbsnapshots_mysqldb"),], "gzip")

    def decompress_files(self, file_paths, compress_algorithm):
        """ This method is to verify and decompress file using specified algorithm
            use md5 to verify files' data intergrity
            the parameter `file_md5` is a dict in which key is file name you want
            to check and value is md5 value
        """
        if compress_algorithm not in ["gzip", "gtar"]:
            raise Exception("Unsupported compress algorithm " + compress_algorithm)

        for file_path in file_paths:
            if compress_algorithm == "gzip":
                with open(file_path, 'rb') as file:
                    file_data = file.read()
                with open(file_path, 'wb') as file:
                    decompass_data = gzip.decompress(file_data)
                    file.write(decompass_data)
            elif compress_algorithm == "gtar":
                with tarfile.open(file_path, 'r') as tar:
                    tar.extractall(path=os.path.dirname(file_path))
