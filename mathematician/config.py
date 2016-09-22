class ThirdPartyKeys:
    Youtube_key = "AIzaSyBvOV3z5LB78NB-yv1osqQQ4A9eY7Xg5r0"

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
    FIELD_COURSE_NAME = "name"
    FIELD_COURSE_YEAR = "year"
    FIELD_COURSE_INSTRUCTOR = "instructor"
    FIELD_COURSE_STATUS = "status"
    FIELD_COURSE_URL = "url"
    FIELD_COURSE_IMAGE = "image"
    FIELD_COURSE_DESCRIPTION = "description"
    FIELD_COURSE_METAINFO = "metaInfo"
    FIELD_COURSE_ENDTIME = "endDate"
    FIELD_COURSE_STARTTIME = "startDate"
    FIELD_COURSE_ORIGINAL_ID = "originalId"
    FIELD_COURSE_STUDENT_LIST = "studentIds"
    FIELD_COURSE_VIDEO_LIST = "videoIds"

    COLLECTION_USER = "users"
    FIELD_USER_NAME = "username"
    FIELD_USER_AGE = "age"
    FIELD_USER_GENDER = "gender"
    FIELD_USER_ORIGINAL_ID = "originalId"
    FIELD_USER_COUNTRY = "country"
    FIELD_USER_COURSE_LIST = "courseIds"
    FIELD_USER_DROPPED_COURSE_LIST = "droppedCourseIds"

    COLLECTION_ENROLLMENT = "enrollments"
    FIELD_ENROLLMENT_COURSE_ID = "courseId"
    FIELD_ENROLLMENT_USER_ID = "userId"
    FIELD_ENROLLMENT_TIMESTAMP = "timestamp"
    FIELD_ENROLLMENT_ACTION = "action"

    COLLECTION_VIDEO = "videos"
    FIELD_VIDEO_COURSE_ID = "courseId"
    FIELD_VIDEO_NAME = "name"
    FIELD_VIDEO_TEMPORAL_HOTNESS = "temporalHotness"
    FIELD_VIDEO_METAINFO = "metaInfo"
    FIELD_VIDEO_SECTION = "section"
    FIELD_VIDEO_DESCRIPTION = "description"
    FIELD_VIDEO_RELEASE_DATE = "releaseDate"
    FIELD_VIDEO_URL = "url"
    FIELD_VIDEO_DURATION = "duration"
    FIELD_VIDEO_ORIGINAL_ID = "originalId"

    COLLECTION_VIDEO_LOG = "videoLogs"
    FIELD_VIDEO_LOG_USER_ID = "userId"
    FIELD_VIDEO_LOG_VIDEO_ID = "videoId"
    FIELD_VIDEO_LOG_TIMESTAMP = "timestamp"
    FIELD_VIDEO_LOG_TYPE = "type"
    FIELD_VIDEO_LOG_METAINFO = "metaInfo"
    FIELD_VIDEO_LOG_ORIGINAL_ID = "originalId"

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
                        FIELD_GENERAL_NAME: FIELD_COURSE_YEAR,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_YEAR,
                        FIELD_GENERAL_VALIDATION: {"$regex": "/[1-9]{4}/"}
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
                        FIELD_GENERAL_NAME: FIELD_COURSE_IMAGE,
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
                        FIELD_GENERAL_NAME: FIELD_COURSE_ORIGINAL_ID,
                        FIELD_GENERAL_VALIDATION: {"$type": "string"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_STUDENT_LIST,
                        FIELD_GENERAL_VALIDATION: {"$type": "array"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_COURSE_VIDEO_LIST,
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
                        FIELD_GENERAL_NAME: FIELD_USER_AGE,
                        FIELD_GENERAL_VALIDATION: {"$gt": 0, "$lt": 120}
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
                        FIELD_GENERAL_NAME: FIELD_USER_COURSE_LIST,
                        FIELD_GENERAL_VALIDATION: {"$type": "array"}
                    },
                    {
                        FIELD_GENERAL_NAME: FIELD_USER_DROPPED_COURSE_LIST,
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
