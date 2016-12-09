class ThirdPartyKeys:
    Youtube_key = "AIzaSyBvOV3z5LB78NB-yv1osqQQ4A9eY7Xg5r0"
    HKMooc_key = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ" + \
    "pc3MiOiJkYXRhYXBpLmhrbW9vYy5oayIsImF1ZCI6InVzZXIudmlzb" + \
    "W9vYyIsImV4cCI6MTUxMDk3NzUxODQwMSwiaWF0IjoxNDc5NDQxNTE" + \
    "4NDAxfQ.MJukG7r-8Sfv6DYWZIGcfZUyDEptkfyHM33rrUaucts "

class FilenameConfig:
    Clickstream_suffix = "-clickstream-log"

class DBConfig:
    DB_HOST = "db_host"
    DB_PORT = "db_port"
    DB_NAME = "db_name"
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

    COLLECTION_VIDEO_LOG = "videoLogs"
    FIELD_VIDEO_LOG_ORIGINAL_ID = "originalId"
    FIELD_VIDEO_LOG_USER_ID = "userId"
    FIELD_VIDEO_LOG_VIDEO_ID = "videoId"
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
                        FIELD_GENERAL_VALIDATION: {"$type": "int"}
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
