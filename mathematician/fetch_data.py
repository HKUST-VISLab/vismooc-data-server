from . import httphelper as http
import tarfile
import gzip
import os
from .config import ThirdPartyKeys, FilenameConfig
from .DB import mongo_dbhelper
from .config import DBConfig as DBC
from datetime import datetime, timezone, timedelta

class DownloadFileFromServer():
    """Download file from server"""
    def __init__(self, save_dir, api_key=ThirdPartyKeys.HKMooc_key, host="https://dataapi2.hkmooc.hk"):
        self.__api_key = api_key
        self.__token = None
        self.__host = host
        self.__http_connection = http.HttpConnection(self.__host)
        self.__lastest_clickstream_time = 0
        self.__db_data = {}
        self.__save_dir = save_dir
        self.get_db_data()

    def get_db_data(self):
        """Fetch meta db files data from db"""
        database = mongo_dbhelper.MongoDB(DBC.DB_HOST, DBC.DB_NAME, DBC.DB_PORT)
        metadbfiles = database.get_collection(DBC.COLLECTION_METADBFILES)
        for item in metadbfiles.find({}):
            self.__db_data[item[DBC.FIELD_METADBFILES_ETAG]] = item
            if item[DBC.FIELD_METADBFILES_TYPE] == DBC.TYPE_CLICKSTREAM and \
                item[DBC.FIELD_METADBFILES_CREATEAT] > self.__lastest_clickstream_time:

                self.__lastest_clickstream_time = item[DBC.FIELD_METADBFILES_CREATEAT]

    def get_token_from_server(self):
        """ Get the Access Token from server using API Key
        """
        self.__http_connection.headers = {"Authorization" : "Token " + self.__api_key}
        response = self.__http_connection.post("/resources/access_tokens", None)
        assert response.get_return_code() == 200
        response_json = response.get_content_json()
        self.__token = response_json.get("collection").get("items")[0].get("accessToken")
        return self.__token
    def get_click_stream(self, start=-1, end=-1, save_dir=None):
        """Get click stream from server, the parameters start
           and end should be unix timestamp in millisecond
        """
        if self.__token is None:
            self.get_token_from_server()
        now = int(datetime.now().timestamp())
        start = self.__lastest_clickstream_time * 1000 if start == -1 else start
        end = now * 1000 * 1000 if end == -1 else end
        save_dir = save_dir or self.__save_dir

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
        self.__http_connection.download_files(file_urls, save_dir, common_suffix=FilenameConfig.Clickstream_suffix)
        file_paths = [os.path.join(save_dir, file_name)+FilenameConfig.Clickstream_suffix for file_name in file_names]
        self.decompress_files(file_paths, "gzip")
        new_metadb_items = [{DBC.FIELD_METADBFILES_CREATEAT : now,
            DBC.FIELD_METADBFILES_FILEPATH : file_path,
            DBC.FIELD_METADBFILES_TYPE : DBC.TYPE_CLICKSTREAM} \
            for file_path in file_paths]
        return new_metadb_items

    def get_mongodb_and_mysqldb_snapshot(self, save_dir=None):
        """ Download mongodb and mysql snapshot
        """
        if self.__token is None:
            self.get_token_from_server()
        save_dir = save_dir or self.__save_dir
        new_metadb_items = []
        now = int(datetime.now().timestamp())
        db_list = [FilenameConfig.MongoDB_Name, FilenameConfig.SQLDB_Name]
        self.__http_connection.headers = {"Authorization" : "Token " + self.__token}
        # self.__http_connection.download_files(file_urls, save_dir,)
        urls = [self.__host + "/resources/" + db_name for \
               db_name in db_list]
        for collection in db_list:
            url = self.__host + "/resources/" + collection
            print(url)
            etag = self.__http_connection.head(url).get_headers().get("ETag")
            if etag and etag not in self.__db_data.keys():
                urls.add(url)
                item = {}
                item[DBC.FIELD_METADBFILES_CREATEAT] = now
                item[DBC.FIELD_METADBFILES_ETAG] = etag
                item[DBC.FIELD_METADBFILES_FILEPATH] = os.path.join(save_dir, collection)
                item[DBC.FIELD_METADBFILES_TYPE] = DBC.TYPE_MONGO if \
                    collection == FilenameConfig.MongoDB_Name else DBC.TYPE_MYSQL
                new_metadb_items.append(item)
        self.__http_connection.download_files(urls, save_dir)
                
        # file_paths = [os.path.join(save_dir, file_name) for file_name in db_list]
        self.decompress_files([os.path.join(save_dir, FilenameConfig.MongoDB_Name),], "gtar")
        self.decompress_files([os.path.join(save_dir, FilenameConfig.SQLDB_Name),], "gzip")
        return new_metadb_items

    def decompress_files(self, file_paths, compress_algorithm):
        """ This method is to verify and decompress file using specified algorithm
            use md5 to verify files' data intergrity
            the parameter `file_md5` is a dict in which key is file name you want
            to check and value is md5 value
        """
        if compress_algorithm not in ["gzip", "gtar"]:
            raise Exception("Unsupported compress algorithm " + compress_algorithm)

        for file_path in file_paths:
            if not os.path.exists(file_path):
                continue
            if compress_algorithm == "gzip":
                with open(file_path, 'rb') as file:
                    file_data = file.read()
                with open(file_path, 'wb') as file:
                    decompass_data = gzip.decompress(file_data)
                    file.write(decompass_data)
            elif compress_algorithm == "gtar":
                with tarfile.open(file_path, 'r') as tar:
                    tar_root = tar.getnames()[0]
                    tar.extractall(path=os.path.dirname(file_path))
                    # print(os.path.join(os.path.dirname(file_path), tar_root))
                    self.bson2json(os.path.join(os.path.dirname(file_path), tar_root))
                    # os.remove(file_path)
    def bson2json(self, dir):
        file_to_be_process = set(['modulestore.active_versions.bson', 'modulestore.structures.bson'])
        files = [os.path.join(dir, file) for file in os.listdir(dir)]
        for file in files:
            if os.path.isdir(file):
                self.bson2json(file)
            elif os.path.isfile(file) and os.path.basename(file) in file_to_be_process:
                cmd = "bsondump " + file + " > " + file[0:file.rindex('.')+1] + 'json'
                output = os.system(cmd)
                # os.remove(file)
