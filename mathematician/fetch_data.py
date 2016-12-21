'''Fetch data from data sources HKMOOC
'''

import tarfile
import gzip
import os
import json
from datetime import datetime
from . import httphelper as http
from .DB import mongo_dbhelper
from .config import DBConfig as DBC, DataSource as DS, ThirdPartyKeys as TPK, FilenameConfig as FC


class DownloadFileFromServer():
    """Download file from server"""

    def __init__(self, save_dir, host=DS.HOST, api_key=TPK.HKMooc_key):
        self.__api_key = api_key
        self.__token = None
        self.__host = host
        self.__http_connection = http.HttpConnection(self.__host)
        self.__lastest_clickstream_time = 0
        self.__db_data = {}
        self.__save_dir = save_dir
        self._db = mongo_dbhelper.MongoDB(DBC.DB_HOST, DBC.DB_NAME, DBC.DB_PORT)
        self.get_db_data()

    def get_db_data(self):
        """Fetch meta db files data from db"""
        if os.path.exists(os.path.join(self.__save_dir, FC.META_DB_RECORD)):
            with open(os.path.join(self.__save_dir, FC.META_DB_RECORD), 'r') as file:
                metadbfiles = json.load(file)
        else:
            metadbfiles = self._db.get_collection(DBC.COLLECTION_METADBFILES).find({})
        for item in metadbfiles:
            self.__db_data[item[DBC.FIELD_METADBFILES_ETAG]] = item
            if item[DBC.FIELD_METADBFILES_TYPE] == DBC.TYPE_CLICKSTREAM and \
                    item[DBC.FIELD_METADBFILES_CREATEAT] > self.__lastest_clickstream_time:
                self.__lastest_clickstream_time = item[
                    DBC.FIELD_METADBFILES_CREATEAT]

    def get_token_from_server(self):
        """ Get the Access Token from server using API Key
        """
        self.__http_connection.headers = {
            "Authorization": "Token " + self.__api_key}
        response = self.__http_connection.post(DS.ACCESS_TOKENS_URL, None)
        assert response.get_return_code() == 200
        response_json = response.get_content_json()
        self.__token = response_json.get("collection").get("items")[
            0].get("accessToken")
        self.__http_connection.headers = {
            "Authorization": "Token " + self.__token}
        return self.__token

    def get_click_stream(self, start=0, end=0, save_dir=None):
        """Get click stream from server, the parameters start
           and end should be unix timestamp in millisecond
        """
        self.get_token_from_server()
        now = int(datetime.now().timestamp())
        start = self.__lastest_clickstream_time * 1000
        end = now * 1000
        save_dir = save_dir or self.__save_dir

        response = self.__http_connection.get(DS.CLICKSTREAMS_URL,
                                              {"since": str(start), "before": str(end)})
        print("Start is " + str(start) + " , End is " + str(end))
        assert response.get_return_code() == 200
        items = response.get_content_json().get("collection")
        items = items.get("items") if items else []
        file_urls = []
        md5s = {}
        for item in items:
            href = item['href']
            file_urls.append(href[href.index(DS.HOST) + len(DS.HOST):])
            md5s[href[href.rindex("/") + 1:]] = item.get('md5')

        print('Begin to download log-files, totally ' + str(len(file_urls)) + " files, please wait")
        downloaded_files = self.__http_connection.download_files(
            file_urls, save_dir, common_suffix=FC.Clickstream_suffix)
        print('Finish downloading log-fiels')
        print('Begin to decompress log-files')
        self.decompress_files(downloaded_files, "gzip")
        print('Finish decompress log-files')
        # cache the metaInfo of log files into database
        print('Begin to cache the metainfo of log-files into mongoDB')
        new_items = [{
            DBC.FIELD_METADBFILES_CREATEAT: now,
            DBC.FIELD_METADBFILES_FILEPATH: file_path,
            DBC.FIELD_METADBFILES_ETAG: md5s.get(file_path[file_path.rindex(os.sep) + 1:]),
            DBC.FIELD_METADBFILES_TYPE: DBC.TYPE_CLICKSTREAM
        } for file_path in downloaded_files if os.path.exists(file_path)]
        # if no log has been downloaded, add a empty log record
        if len(new_items) < 1:
            new_items.append({
                DBC.FIELD_METADBFILES_CREATEAT: now,
                DBC.FIELD_METADBFILES_FILEPATH: save_dir + "/no_log",
                DBC.FIELD_METADBFILES_ETAG: str(now) + "-no_log",
                DBC.FIELD_METADBFILES_TYPE: DBC.TYPE_CLICKSTREAM
            })
        self._db.get_collection(DBC.COLLECTION_METADBFILES).insert_many(new_items)
        print('Finish caching the metainfo of log-files into mongoDB')
        return new_items

    def get_mongodb_and_mysqldb_snapshot(self, save_dir=None):
        """ Download mongodb and mysql snapshot
        """
        self.get_token_from_server()
        save_dir = save_dir or self.__save_dir
        now = int(datetime.now().timestamp())
        new_metadb_items = []

        # check what file should be downloaded
        print("Begin to analyze which snapshots need to be downloaded")
        urls = []
        etag = self.__http_connection.head(
            DS.MONGODB_URL).get_headers().get("ETag")
        if etag and etag not in self.__db_data.keys():
            urls.append(DS.MONGODB_URL)

        etag = self.__http_connection.head(
            DS.SQLDB_URL).get_headers().get("ETag")
        if etag and etag not in self.__db_data.keys():
            urls.append(DS.SQLDB_URL)

        print("Begin to download DB snapshots, totally "+str(len(urls))+" files, pleas wait")
        downloaded_files = self.__http_connection.download_files(urls, save_dir)
        print("Finish download DB snapshots")
        for file_path in downloaded_files:
            if FC.MongoDB_FILE in file_path:
                self.decompress_files([file_path, ], "gtar")
                # cache the file info if  it has been downloaded successfully
                item = {}
                item[DBC.FIELD_METADBFILES_CREATEAT] = now
                item[DBC.FIELD_METADBFILES_ETAG] = etag
                item[DBC.FIELD_METADBFILES_FILEPATH] = os.path.join(
                    save_dir, FC.SQLDB_FILE)
                item[DBC.FIELD_METADBFILES_TYPE] = DBC.TYPE_MYSQL
                new_metadb_items.append(item)
            if FC.SQLDB_FILE in file_path:
                self.decompress_files([file_path, ], "gzip")
                # cache the file info if  it has been downloaded successfully
                item = {}
                item[DBC.FIELD_METADBFILES_CREATEAT] = now
                item[DBC.FIELD_METADBFILES_ETAG] = etag
                item[DBC.FIELD_METADBFILES_FILEPATH] = os.path.join(
                    save_dir, FC.MongoDB_FILE)
                item[DBC.FIELD_METADBFILES_TYPE] = FC.MongoDB_FILE
                new_metadb_items.append(item)
        self._db.get_collection(DBC.COLLECTION_METADBFILES).insert_many(new_metadb_items)
        return new_metadb_items

    def decompress_files(self, file_paths, compress_algorithm):
        """ This method is to verify and decompress file using specified algorithm
            use md5 to verify files' data intergrity
            the parameter `file_md5` is a dict in which key is file name you want
            to check and value is md5 value
        """
        if compress_algorithm not in ["gzip", "gtar"]:
            raise Exception(
                "Unsupported compress algorithm " + compress_algorithm)

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
                    self.bson2json(os.path.join(
                        os.path.dirname(file_path), tar_root))
                    # os.remove(file_path)

    def bson2json(self, dir):
        file_to_be_process = set([FC.ACTIVE_VERSIONS[FC.ACTIVE_VERSIONS.rindex('/')+1:],
                                  FC.STRUCTURES[FC.STRUCTURES.rindex('/')+1:]])
        files = [os.path.join(dir, file) for file in os.listdir(dir)]
        for file in files:
            if os.path.isdir(file):
                self.bson2json(file)
            elif os.path.isfile(file) and os.path.basename(file) in file_to_be_process:
                cmd = "bsondump " + file + " > " + \
                    file[0:file.rindex('.') + 1] + 'json'
                output = os.system(cmd)
                # os.remove(file)
