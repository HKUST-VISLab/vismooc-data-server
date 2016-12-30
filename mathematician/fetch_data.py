'''Fetch data from data sources HKMOOC
'''

import tarfile
import gzip
import os
from datetime import datetime, timedelta
from . import http_helper as http
from .DB import mongo_dbhelper
from .config import DBConfig as DBC, DataSource as DS, ThirdPartyKeys as TPK, FilenameConfig as FC
from .logger import warn, info

FIELD_LAST_MODIFIED = "Last-Modified"
FIELD_ETAG = "ETag"
FIELD_MD5 = "Content-MD5"
FIELD_CONTENT_TYPE = "Content-Type"
class DownloadFileFromServer():
    """Download file from server"""

    def __init__(self, save_dir, host=None, api_key=None, access_token=None):
        self.__api_key = api_key or TPK.HKMooc_key
        self.__token = access_token or TPK.HKMooc_access_token
        self.__host = host or DS.HOST
        self.__http_connection = http.HttpConnection(self.__host)
        self.__latest_clickstream_ts = int((datetime.now() - timedelta(days=1)).timestamp())
        self.__latest_mongo_ts = 0
        self.__latest_mysql_ts = 0
        self.__metainfo_downloaded = {}
        self.__save_dir = save_dir
        self._db = mongo_dbhelper.MongoDB(
            DBC.DB_HOST, DBC.DB_NAME, DBC.DB_PORT)
        self.get_metainfo_downloaded()

    def get_metainfo_downloaded(self):
        """Fetch meta db files data from db"""
        metadbfiles = self._db.get_collection(DBC.COLLECTION_METADBFILES).find({})
        for item in metadbfiles:
            self.__metainfo_downloaded[item[DBC.FIELD_METADBFILES_ETAG]] = item
            file_type = item[DBC.FIELD_METADBFILES_TYPE]
            created_at = item[DBC.FIELD_METADBFILES_CREATEDAT]
            if file_type == DBC.TYPE_CLICKSTREAM and created_at > self.__latest_clickstream_ts:
                self.__latest_clickstream_ts = created_at
            elif file_type == DBC.TYPE_MONGO and created_at > self.__latest_mongo_ts:
                self.__latest_mongo_ts = created_at
            elif file_type == DBC.TYPE_MYSQL and created_at > self.__latest_mysql_ts:
                self.__latest_mysql_ts = created_at

    def set_token(self):
        """ Get the Access Token from server using API Key.
            If cannot get the token sucessfully, this method will return None.
        """
        if self.__token:
            self.__http_connection.headers = {"Authorization": "Token " + self.__token}
            return True
        else:
            self.__http_connection.headers = {"Authorization": "Token " + self.__api_key}
            response = self.__http_connection.post(DS.ACCESS_TOKENS_URL, None)
            if response.get_return_code() == 200:
                try:
                    collection = response.get_content_json().get("collection")
                    items = collection.get("items")
                    self.__token = items[0].get("accessToken")
                except AttributeError as ex:
                    warn("In set_token(), cannot get target attribute from the response")
                    print(ex)
                except TypeError as ex:
                    warn("In set_token(), cannot get the first item from the items")
                    print(ex)
                else:
                    self.__http_connection.headers = {"Authorization": "Token " + self.__token}
                    return True
            else:
                warn("In get_token_from_serer(), the return code of server is not 200")

    def get_click_stream(self, start=0, end=0, save_dir=None):
        """Get click stream from server, the parameters start and end should be unix timestamp in\
           millisecon.
           If the log file is required sucessfully, the metainfo of these files will be returned,
           otherise a NoneType will be returned.
        """
        self.set_token()
        now = int(datetime.now().timestamp())
        start = self.__latest_clickstream_ts * 1000
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
                warn(ex)
            file_urls = []
            try:
                for item in items:
                    href = item.get('href')
                    file_urls.append(href[href.index(DS.HOST) + len(DS.HOST):])
            except AttributeError as ex:
                warn("In get_click_stream(), cannot get target attribute(`href` or `md5`) from\
                     items")
                warn(ex)

            info('Begin to download log-files, totally ' +
                 str(len(file_urls)) + " files, please wait")
            downloaded_files = self.__http_connection.download_files(
                file_urls, save_dir, common_suffix=FC.Clickstream_suffix, retry_time=1)
            info('Finish downloading log-fiels')
            info('Begin to decompress log-files')
            self.decompress_files([response.get_content() for response in downloaded_files], "gzip")
            info('Finish decompress log-files')
            # cache the metaInfo of log files into database
            info('Begin to cache the metainfo of log-files into mongoDB')
            new_meta_items = [{
                DBC.FIELD_METADBFILES_CREATEDAT: now,
                DBC.FIELD_METADBFILES_FILEPATH: res.get_content(),
                DBC.FIELD_METADBFILES_ETAG: res.get_headers().get(FIELD_MD5),
                DBC.FIELD_METADBFILES_TYPE: DBC.TYPE_CLICKSTREAM,
                DBC.FIELD_METADBFILES_LAST_MODIFIED: res.get_headers().get(FIELD_LAST_MODIFIED),
                DBC.FIELD_METADBFILES_PROCESSED: False,
            } for res in downloaded_files if os.path.exists(res.get_content())]
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

    def get_mongo_snapshot(self, save_dir=None):
        '''Get the mongo snapshot
        '''
        self.set_token()
        now = int(datetime.now().timestamp())
        save_dir = save_dir or self.__save_dir
        new_meta_items = []

        # check what file should be downloaded
        info("Begin to analyze which snapshots need to be downloaded")
        urls = []
        md5s = []

        mysql_header = self.__http_connection.head(DS.SQLDB_URL).get_headers()
        mysql_md5 = mysql_header.get(FIELD_MD5)
        mysql_etag = mysql_header.get(FIELD_ETAG)
        mysql_last_modified = mysql_header.get(FIELD_LAST_MODIFIED)
        if mysql_etag and mysql_etag not in self.__metainfo_downloaded.keys():
            urls.append(DS.SQLDB_URL)
            md5s.append(mysql_md5)

        info("Begin to download DB snapshots, totally " + str(len(urls)) + " files, pleas wait")
        file_path = self.__http_connection.download_file(urls[0], save_dir, md5_checksum=md5s[0])
        info("Finish download DB snapshots. The files we downloaded is " + file_path)
        if FC.SQLDB_FILE in file_path:
            info("Begin to decompress the mysql snapshot")
            self.decompress_files([file_path, ], "gzip")
            info("Finish decompressing the mysql snapshot")
            # cache the file info if  it has been downloaded successfully
            item = {
                DBC.FIELD_METADBFILES_CREATEDAT: now,
                DBC.FIELD_METADBFILES_LAST_MODIFIED: mysql_last_modified,
                DBC.FIELD_METADBFILES_ETAG: mysql_etag,
                DBC.FIELD_METADBFILES_FILEPATH: os.path.join(save_dir, FC.SQLDB_FILE),
                DBC.FIELD_METADBFILES_TYPE: DBC.TYPE_MYSQL
            }
            new_meta_items.append(item)
        info('Begin to cache the metainfo of dbsnapshots into mongoDB')
        self._db.get_collection(DBC.COLLECTION_METADBFILES).insert_many(new_meta_items)
        info('Finish caching the metainfo of dbsnapshots into mongoDB')
        return new_meta_items

    def get_mongo_and_mysql_snapshot(self, save_dir=None):
        """ Download mongodb and mysql snapshot
        """
        self.set_token()
        now = int(datetime.now().timestamp())
        save_dir = save_dir or self.__save_dir
        new_meta_items = []

        # check what file should be downloaded
        info("Begin to analyze which snapshots need to be downloaded")
        urls = []
        mongo_header = self.__http_connection.head(DS.MONGODB_URL).get_headers()
        # mongo_md5 = mongo_header.get(FIELD_MD5)
        mongo_etag = mongo_header.get(FIELD_ETAG)
        mongo_last_modified = mongo_header.get(FIELD_LAST_MODIFIED)
        if mongo_etag and mongo_etag not in self.__metainfo_downloaded.keys():
            urls.append(DS.MONGODB_URL)

        mysql_header = self.__http_connection.head(DS.SQLDB_URL).get_headers()
        # mysql_md5 = mysql_header.get(FIELD_MD5)
        mysql_etag = mysql_header.get(FIELD_ETAG)
        mysql_last_modified = mysql_header.get(FIELD_LAST_MODIFIED)
        if mysql_etag and mysql_etag not in self.__metainfo_downloaded.keys():
            urls.append(DS.SQLDB_URL)

        info("Begin to download DB snapshots, totally " + str(len(urls)) + " files, pleas wait")
        downloaded_files = self.__http_connection.download_files(urls, save_dir)
        info("Finish download DB snapshots. The files we downloaded is "+",".join(downloaded_files))
        if len(urls) == 0:
            return new_meta_items
        for response in downloaded_files:
            file_path = response.get_content()
            if FC.MongoDB_FILE in file_path:
                info("Begin to decompress the mongo snapshot")
                self.decompress_files([file_path, ], "gtar")
                info("Finish decompressing the mongo snapshot")
                # cache the file info if  it has been downloaded successfully
                item = {
                    DBC.FIELD_METADBFILES_CREATEDAT: now,
                    DBC.FIELD_METADBFILES_LAST_MODIFIED: mongo_last_modified,
                    DBC.FIELD_METADBFILES_ETAG: mongo_etag,
                    DBC.FIELD_METADBFILES_FILEPATH: os.path.join(save_dir, FC.MongoDB_FILE),
                    DBC.FIELD_METADBFILES_TYPE: DBC.TYPE_MONGO,
                    DBC.FIELD_METADBFILES_PROCESSED: False
                }
                new_meta_items.append(item)
            elif FC.SQLDB_FILE in file_path:
                info("Begin to decompress the mysql snapshot")
                self.decompress_files([file_path, ], "gzip")
                info("Finish decompressing the mysql snapshot")
                # cache the file info if  it has been downloaded successfully
                item = {
                    DBC.FIELD_METADBFILES_CREATEDAT: now,
                    DBC.FIELD_METADBFILES_LAST_MODIFIED: mysql_last_modified,
                    DBC.FIELD_METADBFILES_ETAG: mysql_etag,
                    DBC.FIELD_METADBFILES_FILEPATH: os.path.join(save_dir, FC.SQLDB_FILE),
                    DBC.FIELD_METADBFILES_TYPE: DBC.TYPE_MYSQL,
                    DBC.FIELD_METADBFILES_PROCESSED: False
                }
                new_meta_items.append(item)
        info('Begin to cache the metainfo of dbsnapshots into mongoDB')
        self._db.get_collection(DBC.COLLECTION_METADBFILES).insert_many(new_meta_items)
        info('Finish caching the metainfo of dbsnapshots into mongoDB')
        return new_meta_items

    def decompress_files(self, file_paths, compress_algorithm):
        """ This method is to verify and decompress file using specified algorithm.
        """
        if compress_algorithm not in ["gzip", "gtar"]:
            raise Exception("Unsupported compress algorithm " + compress_algorithm)

        for file_path in file_paths:
            if not os.path.exists(file_path):
                continue
            if compress_algorithm == "gzip":
                chunk_size = 1 << 16
                with gzip.open(file_path, "rb") as file:
                    new_file_path = file_path[:file_path.rindex('.')]
                    file_out = open(new_file_path, "wb")
                    while True:
                        chunk = file.read(chunk_size)
                        if not chunk:
                            break
                        file_out.write(chunk)
                    file_out.close()
                os.remove(file_path)
            elif compress_algorithm == "gtar":
                try:
                    with tarfile.open(file_path, 'r') as tar:
                        tar.extractall(path=os.path.dirname(file_path))
                except tarfile.TarError as ex:
                    warn("Extract file from mongodbsnapshot failed!")
                    warn(ex)
                # self.bson2json(os.path.join(os.path.dirname(file_path), tar_root))

    # def bson2json(self, dir_name):
    #     '''Convert all bson files under specific dir to json files
    #     '''
    #     file_to_be_process = set([FC.ACTIVE_VERSIONS[FC.ACTIVE_VERSIONS.rindex('/') + 1:FC.ACTIVE_VERSIONS.rindex('.')] + ".bson",
    #                               FC.STRUCTURES[FC.STRUCTURES.rindex('/') + 1:FC.STRUCTURES.rindex('.')] + ".bson"])
    #     files = [os.path.join(dir_name, file) for file in os.listdir(dir_name)]
    #     for file in files:
    #         if os.path.isfile(file) and os.path.basename(file) in file_to_be_process:
    #             cmd = "bsondump " + file + " > " + file[0:file.rindex('.') + 1] + 'json'
    #             os.system(cmd)
    #             # os.remove(file)
