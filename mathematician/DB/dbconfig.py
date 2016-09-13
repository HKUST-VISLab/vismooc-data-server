class DBConfig:
    DB_HOST = "db_location"
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
    

    COLLECTION_COURSE = "course"
    FIELD_COURSE_NAME = "name"
    FIELD_COURSE_YEAR = "year"
    FIELD_COURSE_INSTRUCTOR = "instructor"
    FIELD_COURSE_STATUS = "status"
    FIELD_COURSE_URL = "url"
    FIELD_COURSE_IMAGE = "image"
    FIELD_COURSE_DESCRIPTION = "description"
    FIELD_COURSE_METAINFO = "metainfo"
    FIELD_COURSE_ENDTIME = "endTime"
    FIELD_COURSE_STARTTIME = "startTime"
    FIELD_COURSE_ORIGINAL_ID = "original_id"
    FIELD_COURSE_STUDENT_LIST = "student_list"
    FIELD_COURSE_VIDEO_LIST = "video_list"

    COLLECTION_USER = "user"
    FIELD_USER_AGE = "age"
    FIELD_USER_GENDER = "gender"
    FIELD_USER_ORIGINAL_ID = "original_id"
    FIELD_USER_COUNTRY = "country"
    FIELD_USER_COURSE_LIST = "course_list"
    FIELD_USER_DROPPED_COURSE_LIST = "dropped_course_list"

    COLLECTION_ENROLL_HISTORY = "enroll_history"
    FIELD_ENROLL_HISTORY_COURSE_ID = "course_id"
    FIELD_ENROLL_HISTORY_USER_ID = "user_id"
    FIELD_ENROLL_HISTORY_TIMESTAMP = "timestamp"
    FIELD_ENROLL_HISTORY_ACTION = "action"

    COLLECTION_VIDEO = "video"
    FIELD_VIDEO_COURSE_ID = "course_id"
    FIELD_VIDEO_NAME = "name"
    FIELD_VIDEO_TEMPORAL_HOTNESS = "temporal_hotness"
    FIELD_VIDEO_METAINFO = "metainfo"
    FIELD_VIDEO_SECTION = "section"
    FIELD_VIDEO_DESCRIPTION = "description"
    FIELD_VIDEO_RELEASE_DATE = "release_date"
    FIELD_VIDEO_URL = "url"
    FIELD_VIDEO_LENGTH = "length"
    FIELD_VIDEO_ORIGINAL_ID = "original_id"

    COLLECTION_VIDEO_LOG = "video_log"
    FIELD_VIDEO_LOG_USER_ID = "user_id"
    FIELD_VIDEO_LOG_VIDEO_ID = "video_id"
    FIELD_VIDEO_LOG_TIMESTAMP = "timestamp"
    FIELD_VIDEO_LOG_TYPE = "type"
    FIELD_VIDEO_LOG_METAINFO = "metainfo"
    FIELD_VIDEO_LOG_ORIGINAL_ID = "original_id"
    


    CONFIG_JSON = \
    {
        "db_location": "localhost",

        "db_port": 27017,

        "db_name": "vismooc_db",

        "db_user": "vismooc",

        "db_passwd": "vismooc",

        "db_collections":

        [

            {

                "collection_name": "course",

                "fields":

                [

                    {

                        "field_name": "name",

                        "validation": {"$type": "string"}

                    },

                    {

                        "field_name": "year",

                        "validation": {"$regex": "/[1-9]{4}/"}

                    },

                    {

                        "field_name": "instructor",

                        "validation": {"$type": "string"}

                    },

                    {

                        "field_name": "status",

                        "validation": {"$in": ["active", "finish"]}

                    },

                    {

                        "field_name": "url",

                        "validation": {"$type": "string"}

                    },

                    {

                        "field_name": "image",

                        "validation": {"$type": "string"}

                    },

                    {

                        "field_name": "description",

                        "validation": {"$type": "string"}

                    },

                    {

                        "field_name": "metainfo",

                        "validation": {"$type": "object"}

                    },

                    {

                        "field_name": "endTime",

                        "validation": {"$type": "timestamp"}

                    },

                    {

                        "field_name": "startTime",

                        "validation": {"$type": "timestamp"}

                    },

                    {

                        "field_name": "original_id",

                        "validation": {"$type": "string"}

                    },

                    {

                        "field_name": "student_list",

                        "validation": {"$type": "array"}

                    },

                    {

                        "field_name": "video_list",

                        "validation": {"$type": "array"}

                    }

                ],

                "index":

                [

                    {

                        "field_name": "name",

                        "order": 1

                    },

                    {

                        "field_name": "status",

                        "order": 1

                    }

                ]

            },

            {

                "collection_name": "user",

                "fields":

                [

                    {

                        "field_name": "name",

                        "validation": {"$type": "string"}

                    },

                    {

                        "field_name": "age",

                        "validation": {"$gt": 0, "$lt": 120}

                    },

                    {

                        "field_name": "gender",

                        "validation": {"$in": ["male", "female"]}

                    },

                    {

                        "field_name": "original_id",

                        "validation": {"$type": "string"}

                    },

                    {

                        "field_name": "country",

                        "validation": {"$type": "string"}

                    },

                    {

                        "field_name": "course_list",

                        "validation": {"$type": "array"}

                    },

                    {

                        "field_name": "dropped_course_list",

                        "validation": {"$type": "array"}

                    }

                ],
                "index":

                [

                    {

                        "field_name": "country",

                        "order": 1

                    }

                ]



            },

            {

                "collection_name": "enroll_history",

                "fields":

                [

                    {

                        "field_name": "course_id",

                        "validation": {"$type": "objectId"}

                    },

                    {

                        "field_name": "user_id",

                        "validation": {"$type": "objectId"}

                    },

                    {

                        "field_name": "timestamp",

                        "validation": {"$type": "timestamp"}

                    },

                    {

                        "field_name": "action",

                        "validation": {"$in": ["TODO", "TODO"]}

                    }

                ],

                "index":
                [

                    {

                        "field_name": "timestamp",

                        "order": 1

                    }

                ]

            },

            {

                "collection_name": "video",

                "fields":
                [

                    {

                        "field_name": "course_id",

                        "validation": {"$type": "objectId"}

                    },

                    {

                        "field_name": "name",

                        "validation": {"$type": "string"}

                    },

                    {

                        "field_name": "temporal_hotness",

                        "validation": {"$type": "int"}

                    },

                    {

                        "field_name": "metainfo",

                        "validation": {"$type": "object"}

                    },

                    {

                        "field_name": "section",

                        "validation": {"$type": "string"}

                    },

                    {

                        "field_name": "description",

                        "validation": {"$type": "string"}

                    },

                    {

                        "field_name": "release_date",

                        "validation": {"$type": "timestamp"}

                    },

                    {

                        "field_name": "url",

                        "validation": {"$type": "string"}

                    },

                    {

                        "field_name": "length",

                        "validation": {"$type": "int"}

                    },

                    {

                        "field_name": "original_id",

                        "validation": {"$type": "string"}

                    }

                ],

                "index":
                [

                    {

                        "field_name": "course_id",

                        "order": 1

                    },

                    {

                        "field_name": "original_id",

                        "order": 1

                    }

                ]

            },

            {

                "collection_name": "video_log",

                "fields":
                [

                    {

                        "field_name": "user_id",

                        "validation": {"$type": "objectId"}

                    },

                    {

                        "field_name": "video_id",

                        "validation": {"$type": "objectId"}

                    },

                    {

                        "field_name": "timestamp",

                        "validation": {"$type": "timestamp"}

                    },

                    {

                        "field_name": "type",

                        "validation": {"$in": ["TODO", "TODO"]}

                    },

                    {

                        "field_name": "metainfo",

                        "validation": {"$type": "object"}

                    },

                    {

                        "field_name": "original_id",

                        "validation": {"$type": "string"}

                    }

                ],

                "index":
                [

                    {

                        "field_name": "user_id",

                        "order": 1

                    },

                    {

                        "field_name": "video_id",

                        "order": 1

                    },

                    {

                        "field_name": "original_id",

                        "order": 1

                    }

                ]

            }

        ]

    }



    

