'''Fetch data from data sources HKMOOC
'''

import tarfile
import gzip
import os
from datetime import datetime
from . import http_helper as http
from .DB import mongo_dbhelper
from .config import DBConfig as DBC, DataSource as DS, ThirdPartyKeys as TPK, FilenameConfig as FC
from .logger import warn, info


class DownloadFileFromServer():
    """Download file from server"""

    def __init__(self, save_dir, host=DS.HOST, api_key=TPK.HKMooc_key):
        self.__api_key = api_key
        self.__token = None
        self.__host = host
        self.__http_connection = http.HttpConnection(self.__host)
        self.__lastest_clickstream_time = 0
        self.__metainfo_downloaded = {}
        self.__save_dir = save_dir
        self._db = mongo_dbhelper.MongoDB(
            DBC.DB_HOST, DBC.DB_NAME, DBC.DB_PORT)
        self.get_metainfo_downloaded()

    def get_metainfo_downloaded(self):
        """Fetch meta db files data from db"""
        metadbfiles = self._db.get_collection(
            DBC.COLLECTION_METADBFILES).find({})
        for item in metadbfiles:
            self.__metainfo_downloaded[item[DBC.FIELD_METADBFILES_ETAG]] = item
            if item[DBC.FIELD_METADBFILES_TYPE] == DBC.TYPE_CLICKSTREAM and \
                    item[DBC.FIELD_METADBFILES_CREATEAT] > self.__lastest_clickstream_time:
                self.__lastest_clickstream_time = item[
                    DBC.FIELD_METADBFILES_CREATEAT]

    def get_token_from_server(self):
        """ Get the Access Token from server using API Key.
            If cannot get the token sucessfully, this method will return None.
        """
        self.__http_connection.headers = {
            "Authorization": "Token " + self.__api_key}
        response = self.__http_connection.post(DS.ACCESS_TOKENS_URL, None)
        if response.get_return_code() == 200:
            try:
                collection = response.get_content_json().get("collection")
                items = collection.get("items")
                self.__token = items[0].get("accessToken")
            except AttributeError as ex:
                warn("In get_token_from_server(), cannot get target attribute from the\
                      response")
                print(ex)
            except TypeError as ex:
                warn(
                    "In get_token_from_server(), cannot get the first item from the items")
                print(ex)
            else:
                self.__http_connection.headers = {
                    "Authorization": "Token " + self.__token}
                return self.__token
        else:
            warn("In get_token_from_serer(), the return code of server is not 200")

    def get_click_stream(self, start=0, end=0, save_dir=None):
        """Get click stream from server, the parameters start and end should be unix timestamp in\
           millisecon.
           If the log file is required sucessfully, the metainfo of these files will be returned,
           otherise a NoneType will be returned.
        """
        self.get_token_from_server()
        now = int(datetime.now().timestamp())
        start = self.__lastest_clickstream_time * 1000
        end = now * 1000
        save_dir = save_dir or self.__save_dir

        info("Start is " + str(start) + " , End is " + str(end))
        response = self.__http_connection.get(DS.CLICKSTREAMS_URL,
                                              {"since": str(start), "before": str(end)})
        if response.get_return_code() == 200:
            try:
                collection = response.get_content_json().get("collection")
                items = collection.get("items")
            except AttributeError as ex:
                warn("In get_click_stream(), cannot get target attribute(`collection` or `items`)\
                     from the response")
                print(ex)
            file_urls = []
            md5s = {}
            try:
                for item in items:
                    href = item.get('href')
                    file_urls.append(href[href.index(DS.HOST) + len(DS.HOST):])
                    md5s[href[href.rindex("/") + 1:]] = item.get('md5')
            except AttributeError as ex:
                warn("In get_click_stream(), cannot get target attribute(`href` or `md5`) from\
                     items")
                print(ex)

            info('Begin to download log-files, totally ' +
                 str(len(file_urls)) + " files, please wait")
            downloaded_files = self.__http_connection.download_files(
                file_urls, save_dir, common_suffix=FC.Clickstream_suffix)
            info('Finish downloading log-fiels')
            info('Begin to decompress log-files')
            self.decompress_files(downloaded_files, "gzip")
            info('Finish decompress log-files')
            # cache the metaInfo of log files into database
            info('Begin to cache the metainfo of log-files into mongoDB')
            new_meta_items = [{
                DBC.FIELD_METADBFILES_CREATEAT: now,
                DBC.FIELD_METADBFILES_FILEPATH: file_path,
                DBC.FIELD_METADBFILES_ETAG: md5s.get(file_path[file_path.rindex(os.sep) + 1:]),
                DBC.FIELD_METADBFILES_TYPE: DBC.TYPE_CLICKSTREAM
            } for file_path in downloaded_files if os.path.exists(file_path)]
            # if no log has been downloaded, add a empty log record
            if len(new_meta_items) > 1:
                self._db.get_collection(DBC.COLLECTION_METADBFILES).insert_many(new_meta_items)
            else:
                info('No log file has been downloaded, maybe the url is invalided or no log file\
                     has been generated yet')
            info('Finish caching the metainfo of log-files into mongoDB')
            return new_meta_items
        else:
            warn("In get_click_stream(), the return code of server is not 200")

    def get_mongo_and_mysql_snapshot(self, save_dir=None):
        """ Download mongodb and mysql snapshot
        """
        self.get_token_from_server()
        now = int(datetime.now().timestamp())
        save_dir = save_dir or self.__save_dir
        new_meta_items = []

        # check what file should be downloaded
        info("Begin to analyze which snapshots need to be downloaded")
        urls = []
        mongo_etag = self.__http_connection.head(DS.MONGODB_URL).get_headers().get("ETag")
        if mongo_etag and mongo_etag not in self.__metainfo_downloaded.keys():
            urls.append(DS.MONGODB_URL)

        mysql_etag = self.__http_connection.head(DS.SQLDB_URL).get_headers().get("ETag")
        if mysql_etag and mysql_etag not in self.__metainfo_downloaded.keys():
            urls.append(DS.SQLDB_URL)

        info("Begin to download DB snapshots, totally " + str(len(urls)) + " files, pleas wait")
        downloaded_files = self.__http_connection.download_files(urls, save_dir)
        info("Finish download DB snapshots. The files we downloaded is " +
             ",".join(downloaded_files))
        for file_path in downloaded_files:
            if FC.MongoDB_FILE in file_path:
                info("Begin to decompress the mongo snapshot")
                self.decompress_files([file_path, ], "gtar")
                info("Finish decompressing the mongo snapshot")
                # cache the file info if  it has been downloaded successfully
                item = {
                    DBC.FIELD_METADBFILES_CREATEAT: now,
                    DBC.FIELD_METADBFILES_ETAG: mongo_etag,
                    DBC.FIELD_METADBFILES_FILEPATH: os.path.join(save_dir, FC.MongoDB_FILE),
                    DBC.FIELD_METADBFILES_TYPE: DBC.TYPE_MONGO
                }
                new_meta_items.append(item)
            if FC.SQLDB_FILE in file_path:
                info("Begin to decompress the mysql snapshot")
                self.decompress_files([file_path, ], "gzip")
                info("Finish decompressing the mysql snapshot")
                # cache the file info if  it has been downloaded successfully
                item = {
                    DBC.FIELD_METADBFILES_CREATEAT: now,
                    DBC.FIELD_METADBFILES_ETAG: mysql_etag,
                    DBC.FIELD_METADBFILES_FILEPATH: os.path.join(save_dir, FC.SQLDB_FILE),
                    DBC.FIELD_METADBFILES_TYPE: DBC.TYPE_MYSQL
                }
                new_meta_items.append(item)
        info('Begin to cache the metainfo of dbsnapshots into mongoDB')
        self._db.get_collection(DBC.COLLECTION_METADBFILES).insert_many(new_meta_items)
        info('Finish caching the metainfo of dbsnapshots into mongoDB')
        return new_meta_items

    def decompress_files(self, file_paths, compress_algorithm):
        # TODO try to optimize this part of code, add Exception handler
        """ This method is to verify and decompress file using specified algorithm.
            Use md5 to verify files' data intergrity.
            The parameter `file_md5` is a dict in which key is file name you want
            to check and value is md5 value.
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
                    self.bson2json(os.path.join(os.path.dirname(file_path), tar_root))

    def bson2json(self, dir_name):
        # TODO try to optimize this part of code, add Exception handler
        '''Convert all bson files under specific dir to json files
        '''
        file_to_be_process = set([FC.ACTIVE_VERSIONS[FC.ACTIVE_VERSIONS.rindex('/') + 1:FC.ACTIVE_VERSIONS.rindex('.')] + ".bson",
                                  FC.STRUCTURES[FC.STRUCTURES.rindex('/') + 1:FC.STRUCTURES.rindex('.')] + ".bson"])
        files = [os.path.join(dir_name, file) for file in os.listdir(dir_name)]
        for file in files:
            if os.path.isfile(file) and os.path.basename(file) in file_to_be_process:
                cmd = "bsondump " + file + " > " + file[0:file.rindex('.') + 1] + 'json'
                os.system(cmd)
                # os.remove(file)
