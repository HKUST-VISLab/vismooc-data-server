'''All the config fields of data server
'''
import json

class ThirdPartyKeys:
    '''Third party keys
    '''
    Youtube_key = None
    HKMooc_access_token = None
    HKMooc_key = None

class FilenameConfig:
    '''File names of raw data
    '''
    Clickstream_suffix = "-clickstream-log"
    Data_dir = "/vismooc-test-data/"
    MongoDB_FILE = "dbsnapshots_mongodb"
    SQLDB_FILE = "dbsnapshots_mysqldb"
    META_DB_RECORD = "meta_db_record"
    ACTIVE_VERSIONS = "mongodb/edxapp/modulestore.active_versions.json"
    STRUCTURES = "mongodb/edxapp/modulestore.structures.json"

class DataSource:
    '''Urls of datasources
    '''
    HOST = "https://dataapi2.hkmooc.hk"
    ACCESS_TOKENS_URL = "/resources/access_tokens"
    CLICKSTREAMS_URL = "/resources/clickstreams"
    MONGODB_URL = "/resources/dbsnapshots_mongodb"
    SQLDB_URL = "/resources/dbsnapshots_mysqldb"

class DBConfig:
    '''Config of database
    '''
    DB_HOST = "localhost"
    DB_PORT = 27017
    DB_NAME = "test-vismooc-newData-temp"
    DB_USER = "db_user"
    DB_PASSWD = "db_passwd"
    DB_GENERAL_COLLECTIONS = "db_collections"
    COLLECTION_GENERAL_NAME = "collection_name"
    COLLECTION_GENERAL_FIELDS = "fields"
    COLLECTION_GENERAL_INDEX = "index"
    FIELD_GENERAL_NAME = "field_name"
    FIELD_GENERAL_VALIDATION = "validation"
    INDEX_GENERAL_INDEX_ORDER = "order"

    COLLECTION_COURSE = "courses"
    FIELD_COURSE_ORIGINAL_ID = "originalId"
    FIELD_COURSE_NAME = "name"
    FIELD_COURSE_YEAR = "year"
    FIELD_COURSE_ORG = "org"
    FIELD_COURSE_ADVERTISED_START = "advertisedStart"
    FIELD_COURSE_INSTRUCTOR = "instructor"
    FIELD_COURSE_STATUS = "status"
    FIELD_COURSE_URL = "url"
    FIELD_COURSE_IMAGE_URL = "courseImageUrl"
    FIELD_COURSE_LOWEST_PASSING_GRADE = "lowest_passing_grade"
    FIELD_COURSE_MOBILE_AVAILABLE = "mobileAvailable"
    FIELD_COURSE_DESCRIPTION = "description"
    FIELD_COURSE_METAINFO = "metaInfo"
    FIELD_COURSE_STARTTIME = "startDate"
    FIELD_COURSE_ENDTIME = "endDate"
    FIELD_COURSE_ENROLLMENT_START = "enrollmentStart"
    FIELD_COURSE_ENROLLMENT_END = "enrollmentEnd"
    FIELD_COURSE_STUDENT_IDS = "studentIds"
    FIELD_COURSE_VIDEO_IDS = "videoIds"
    FIELD_COURSE_DISPLAY_NUMBER_WITH_DEFAULT = "displayNumberWithDefault"

    COLLECTION_USER = "users"
    FIELD_USER_ORIGINAL_ID = "originalId"
    FIELD_USER_USER_NAME = "username"
    FIELD_USER_NAME = "name"
    FIELD_USER_LANGUAGE = "language"
    FIELD_USER_LOCATION = "location"
    FIELD_USER_BIRTH_DATE = "birthDate"
    FIELD_USER_EDUCATION_LEVEL = "educationLevel"
    FIELD_USER_BIO = "bio"
    FIELD_USER_GENDER = "gender"
    FIELD_USER_COUNTRY = "country"
    FIELD_USER_COURSE_IDS = "courseIds"
    FIELD_USER_DROPPED_COURSE_IDS = "droppedCourseIds"
    FIELD_USER_COURSE_ROLE = "courseRoles"

    COLLECTION_ENROLLMENT = "enrollments"
    FIELD_ENROLLMENT_COURSE_ID = "courseId"
    FIELD_ENROLLMENT_USER_ID = "userId"
    FIELD_ENROLLMENT_TIMESTAMP = "timestamp"
    FIELD_ENROLLMENT_ACTION = "action"

    COLLECTION_VIDEO = "videos"
    FIELD_VIDEO_ORIGINAL_ID = "originalId"
    FIELD_VIDEO_COURSE_ID = "courseId"
    FIELD_VIDEO_NAME = "name"
    FIELD_VIDEO_TEMPORAL_HOTNESS = "temporalHotness"
    FIELD_VIDEO_METAINFO = "metaInfo"
    FIELD_VIDEO_SECTION = "section"
    FIELD_VIDEO_DESCRIPTION = "description"
    FIELD_VIDEO_RELEASE_DATE = "releaseDate"
    FIELD_VIDEO_URL = "url"
    FIELD_VIDEO_DURATION = "duration"

    COLLECTION_VIDEO_LOG = "logs"
    FIELD_VIDEO_LOG_ORIGINAL_ID = "originalId"
    FIELD_VIDEO_LOG_USER_ID = "userId"
    FIELD_VIDEO_LOG_VIDEO_ID = "videoId"
    FIELD_VIDEO_LOG_COURSE_ID = "courseId"
    FIELD_VIDEO_LOG_TIMESTAMP = "timestamp"
    FIELD_VIDEO_LOG_TYPE = "type"
    FIELD_VIDEO_LOG_METAINFO = "metaInfo"

    COLLECTION_VIDEO_DENSELOGS = "denselogs"
    FIELD_VIDEO_DENSELOGS_VIDEO_ID = "videoId"
    FIELD_VIDEO_DENSELOGS_COURSE_ID = "courseId"
    FIELD_VIDEO_DENSELOGS_TIMESTAMP = "timestamp"
    FIELD_VIDEO_DENSELOGS_CLICKS = "clicks"
    FIELD_VIDEO_DENSELOGS_ORIGINAL_ID = "originalId"
    FIELD_VIDEO_DENSELOGS_TYPE = "type"
    FIELD_VIDEO_DENSELOGS_USER_ID = "userId"
    FIELD_VIDEO_DENSELOGS_PATH = "path"
    FIELD_VIDEO_DENSELOGS_CODE = "code"
    FIELD_VIDEO_DENSELOGS_CURRENT_TIME = "currentTime"
    FIELD_VIDEO_DENSELOGS_NEW_TIME = "newTime"
    FIELD_VIDEO_DENSELOGS_OLD_TIME = "oldTime"
    FIELD_VIDEO_DENSELOGS_NEW_SPEED = "newSpeed"
    FIELD_VIDEO_DENSELOGS_OLD_SPEED = "oldSpeed"

    COLLECTION_VIDEO_RAWLOGS = "rawlogs"

    COLLECTION_METADBFILES = "metadbfiles"
    FIELD_METADBFILES_ETAG = "etag"
    FIELD_METADBFILES_CREATEAT = "createAt"
    # FIELD_METADBFILES_NAME = "name"
    FIELD_METADBFILES_TYPE = "type"
    FIELD_METADBFILES_FILEPATH = "filepath"

    TYPE_MYSQL = "mysql"
    TYPE_MONGO = "mongo"
    TYPE_CLICKSTREAM = "clickstream"

    CONFIG_JSON = {
        DB_HOST: "localhost",
        DB_PORT: 27017,
        DB_NAME: "vismooc_db",
        DB_USER: "vismooc",
        DB_PASSWD: "vismooc",
        DB_GENERAL_COLLECTIONS:
        [
            {
                COLLECTION_GENERAL_NAME: COLLECTION_COURSE,
                COLLECTION_GENERAL_FIELDS:
                [
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_NAME,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_YEAR,
                        FIELD_GENERAL_VALIDATION: {"$regex": "/[1-9]{4}/"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_ORG,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_ADVERTISED_START,
                        FIELD_GENERAL_VALIDATION: {"$type": "timestamp"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_LOWEST_PASSING_GRADE,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_MOBILE_AVAILABLE,
                        FIELD_GENERAL_VALIDATION: {"$type": "boolean"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_INSTRUCTOR,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_STATUS,
                        FIELD_GENERAL_VALIDATION: {"$in": ["active", "finish"]}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_URL,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_IMAGE_URL,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_DESCRIPTION,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_METAINFO,
                        FIELD_GENERAL_VALIDATION: {"$type": "object"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_ENDTIME,
                        FIELD_GENERAL_VALIDATION: {"$type": "timestamp"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_STARTTIME,
                        FIELD_GENERAL_VALIDATION: {"$type": "timestamp"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_ENROLLMENT_START,
                        FIELD_GENERAL_VALIDATION: {"$type": "timestamp"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_ENROLLMENT_END,
                        FIELD_GENERAL_VALIDATION: {"$type": "timestamp"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_ORIGINAL_ID,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_DISPLAY_NUMBER_WITH_DEFAULT,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_STUDENT_IDS,
                        FIELD_GENERAL_VALIDATION: {"$type": "array"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_VIDEO_IDS,
                        FIELD_GENERAL_VALIDATION: {"$type": "array"}
                    }
                ],
                COLLECTION_GENERAL_INDEX:
                [
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_NAME,
                        INDEX_GENERAL_INDEX_ORDER: 1
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_STATUS,
                        INDEX_GENERAL_INDEX_ORDER: 1
                    }
                ]
            },
            {
                COLLECTION_GENERAL_NAME: COLLECTION_USER,
                COLLECTION_GENERAL_FIELDS:
                [
                    {
                        FIELD_GENERAL_NAME: FIELD_USER_NAME,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_USER_USER_NAME,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_USER_LANGUAGE,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_USER_LOCATION,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_USER_BIRTH_DATE,
                        FIELD_GENERAL_VALIDATION: {"$type": "timestamp"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_USER_EDUCATION_LEVEL,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_USER_BIO,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_USER_GENDER,
                        FIELD_GENERAL_VALIDATION: {"$in": ["male", "female"]}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_USER_ORIGINAL_ID,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_USER_COUNTRY,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_USER_COURSE_IDS,
                        FIELD_GENERAL_VALIDATION: {"$type": "array"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_USER_DROPPED_COURSE_IDS,
                        FIELD_GENERAL_VALIDATION: {"$type": "array"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_USER_COURSE_ROLE,
                        FIELD_GENERAL_VALIDATION: {"$type": "object"}
                    }
                ],
                COLLECTION_GENERAL_INDEX:
                [
                    {
                        FIELD_GENERAL_NAME: FIELD_USER_COUNTRY,
                        INDEX_GENERAL_INDEX_ORDER: 1
                    }
                ]
            },
            {
                COLLECTION_GENERAL_NAME: COLLECTION_ENROLLMENT,
                COLLECTION_GENERAL_FIELDS:
                [
                    {
                        FIELD_GENERAL_NAME: FIELD_ENROLLMENT_COURSE_ID,
                        FIELD_GENERAL_VALIDATION: {"$type": "objectId"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_ENROLLMENT_USER_ID,
                        FIELD_GENERAL_VALIDATION: {"$type": "objectId"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_ENROLLMENT_TIMESTAMP,
                        FIELD_GENERAL_VALIDATION: {"$type": "timestamp"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_ENROLLMENT_ACTION,
                        FIELD_GENERAL_VALIDATION: {"$in": ["TODO", "TODO"]}
                    }
                ],
                COLLECTION_GENERAL_INDEX:
                [
                    {
                        FIELD_GENERAL_NAME: FIELD_ENROLLMENT_TIMESTAMP,
                        INDEX_GENERAL_INDEX_ORDER: 1
                    }
                ]
            },
            {
                COLLECTION_GENERAL_NAME: COLLECTION_VIDEO,
                COLLECTION_GENERAL_FIELDS:
                [
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_COURSE_ID,
                        FIELD_GENERAL_VALIDATION: {"$type": "objectId"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_NAME,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_TEMPORAL_HOTNESS,
                        FIELD_GENERAL_VALIDATION: {"$type": "object"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_METAINFO,
                        FIELD_GENERAL_VALIDATION: {"$type": "object"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_SECTION,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_DESCRIPTION,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_RELEASE_DATE,
                        FIELD_GENERAL_VALIDATION: {"$type": "timestamp"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_URL,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_DURATION,
                        FIELD_GENERAL_VALIDATION: {"$type": "int"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_ORIGINAL_ID,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    }
                ],
                COLLECTION_GENERAL_INDEX:
                [
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_COURSE_ID,
                        INDEX_GENERAL_INDEX_ORDER: 1
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_ORIGINAL_ID,
                        INDEX_GENERAL_INDEX_ORDER: 1
                    }
                ]
            },
            {
                COLLECTION_GENERAL_NAME: COLLECTION_VIDEO_LOG,
                COLLECTION_GENERAL_FIELDS:
                [
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_LOG_USER_ID,
                        FIELD_GENERAL_VALIDATION: {"$type": "objectId"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_LOG_VIDEO_ID,
                        FIELD_GENERAL_VALIDATION: {"$type": "objectId"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_LOG_COURSE_ID,
                        FIELD_GENERAL_VALIDATION: {"$type": "objectId"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_LOG_TIMESTAMP,
                        FIELD_GENERAL_VALIDATION: {"$type": "timestamp"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_LOG_TYPE,
                        FIELD_GENERAL_VALIDATION: {"$in": ["TODO", "TODO"]}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_LOG_METAINFO,
                        FIELD_GENERAL_VALIDATION: {"$type": "object"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_LOG_ORIGINAL_ID,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    }
                ],
                COLLECTION_GENERAL_INDEX:
                [
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_LOG_USER_ID,
                        INDEX_GENERAL_INDEX_ORDER: 1
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_LOG_VIDEO_ID,
                        INDEX_GENERAL_INDEX_ORDER: 1
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_VIDEO_LOG_ORIGINAL_ID,
                        INDEX_GENERAL_INDEX_ORDER: 1
                    }
                ]
            }
        ]
    }


def init_config(config_file_path):
    ''' Init all the configuration from a config file
    '''

    with open(config_file_path, 'r') as file:
        try:
            config_json = json.load(file)
        except json.JSONDecodeError as ex:
            print('Decode init json file failed')
            print(ex.msg)
            return

    mongo_config = config_json.get('mongo')
    # init mongo db
    if mongo_config:
        DBConfig.DB_HOST = mongo_config.get('host') or DBConfig.DB_HOST
        DBConfig.DB_NAME = mongo_config.get('name') or DBConfig.DB_NAME
        DBConfig.DB_PORT = mongo_config.get('port') or DBConfig.DB_PORT

    dataserver_config = config_json.get('data_server')
    if dataserver_config:
        # init data server sources
        data_sources_config = dataserver_config.get('data_sources')
        if data_sources_config:
            DataSource.HOST = data_sources_config.get(
                'data_source_host') or DataSource.HOST
            DataSource.ACCESS_TOKENS_URL = data_sources_config.get(
                '/resources/access_tokens') or DataSource.ACCESS_TOKENS_URL
            DataSource.CLICKSTREAMS_URL = data_sources_config.get(
                'clickstreams_url') or DataSource.CLICKSTREAMS_URL
            DataSource.MONGODB_URL = data_sources_config.get(
                'mongoDB_url') or DataSource.MONGODB_URL
            DataSource.SQLDB_URL = data_sources_config.get(
                'SQLDB_url') or DataSource.SQLDB_URL

        # init the data file NameError
        data_filenames = dataserver_config.get("data_filenames")
        if data_filenames:
            FilenameConfig.Data_dir = data_filenames.get("data_dir")
            FilenameConfig.MongoDB_FILE = data_filenames.get(
                "mongodb_file") or FilenameConfig.MongoDB_FILE
            FilenameConfig.SQLDB_FILE = data_filenames.get(
                "sqldb_file") or FilenameConfig.SQLDB_FILE
            FilenameConfig.META_DB_RECORD = data_filenames.get(
                "meta_db_record") or FilenameConfig.META_DB_RECORD
            FilenameConfig.ACTIVE_VERSIONS = data_filenames.get(
                "active_versions") or FilenameConfig.ACTIVE_VERSIONS
            FilenameConfig.STRUCTURES = data_filenames.get(
                "structures") or FilenameConfig.STRUCTURES

        # init 3rd party keys
        third_party_keys = dataserver_config.get('third_party_keys')
        if third_party_keys:
            ThirdPartyKeys.Youtube_key = third_party_keys.get(
                'Youtube_key') or ThirdPartyKeys.Youtube_key
            ThirdPartyKeys.HKMooc_key = third_party_keys.get(
                'HKMOOC_key') or ThirdPartyKeys.HKMooc_key
            ThirdPartyKeys.HKMooc_access_token = third_party_keys.get('HKMOOC_access_token') or ThirdPartyKeys.HKMooc_access_token
