'''Process the new hkmooc data
'''

import re
import json
import io
import struct
import queue
import urllib
from os.path import isfile
from datetime import timedelta, datetime
# from bson import ObjectId
import bson

from ..logger import warn, info
from ..http_helper import get as http_get, get_list as http_get_list
from ..pipe import PipeModule
from ..DB.mongo_dbhelper import MongoDB
from ..config import DBConfig as DBC, ThirdPartyKeys as TPK, FilenameConfig as FC

RD_DATA = "data"
RD_DB = "database"
RD_COURSE_IN_MONGO = "course_in_mongo"

def split(text, separator=','):
    """split a text with `separtor`"""

    tmp_stack = []
    results = []
    quota_number = 0
    for i, letter in enumerate(text):
        if letter == "'" and text[i - 1] != "\\":
            quota_number += 1
        elif letter == separator and (quota_number == 2 or quota_number == 0):
            results.append("".join(tmp_stack))
            tmp_stack = []
            quota_number = 0
        else:
            tmp_stack.append(letter)

        if i == len(text) - 1:
            results.append("".join(tmp_stack))
            return results

def try_parse_date(date_str, pattern="%Y-%m-%d %H:%M:%S.%f"):
    '''Try to parse a date string and get the timestamp
        If can not parse the string based on certain pattern, return None
    '''
    try:
        return datetime.strptime(date_str, pattern).timestamp()
    except ValueError:
        return None

class ExtractRawData(PipeModule):
    """ Preprocess the file to extract different lines for different tables
    """
    order = 0

    def __init__(self):
        super().__init__()

    def process(self, raw_data, raw_data_filenames=None):
        info("Processing ExtractRawData")

        pattern_insert = r'^INSERT INTO `(?P<table_name>\w*)`'
        pattern_create_db = r'^USE `(?P<db_name>\w*)`;$'
        re_pattern_insert_table = re.compile(pattern_insert)
        re_pattern_create_db = re.compile(pattern_create_db)
        current_db = None
        structureids = set()
        structureid_to_courseid = {}
        courseid_to_structure = {}
        block_queue = queue.Queue()
        module_structure_filename = None

        filenames = [filename for filename in raw_data_filenames if isfile(filename)]
        for filename in filenames:
            if FC.SQLDB_FILE in filename:
                with open(filename, 'r', encoding='utf-8') as file:
                    for line in file:
                        match_db = re_pattern_create_db.search(line)
                        # find out the current database
                        if match_db is not None:
                            current_db = match_db.group("db_name")
                        if (current_db is None) or (current_db != "edxapp"):
                            continue
                        # if in database edxapp
                        match_table = re_pattern_insert_table.search(line)
                        if match_table is None:
                            continue
                        # remove first '(' and last ';)\n' pay attention to '\n'
                        line = line[line.index('(') + 1: -3]
                        records = line.split('),(')
                        table_name = match_table.group("table_name")
                        raw_data[table_name] = records
            elif FC.ACTIVE_VERSIONS in filename:
                with open(filename, 'rb') as file:
                    for record in bson.decode_file_iter(file):
                        # record = json.loads(line)
                        versions = record.get('versions')
                        if versions is None:
                            continue
                        published_branch = versions.get('published-branch')
                        if published_branch is None:
                            continue
                        oid = str(published_branch)
                        structureids.add(oid)
                        structureid_to_courseid[oid] = record['org'] + '+' + \
                            record['course'].replace('.', '_') + '+' + record['run']
            elif FC.STRUCTURES in filename:
                module_structure_filename = filename
        # modulestore.active_version must be processed before modulestore.structures
        if module_structure_filename and len(structureid_to_courseid) > 0:
            with open(module_structure_filename, 'rb') as file:
                for record in bson.decode_file_iter(file):
                    oid = str(record.get('_id'))
                    if oid in structureids:
                        courseid_to_structure[structureid_to_courseid[oid]] = record
            for structure in courseid_to_structure.values():
                blocks_dict = {}
                # construct a dictory which contains all blocks and get the root course block
                blocks = structure.get("blocks")
                for block in blocks:
                    blocks_dict[block.get("block_id")] = block
                    if block.get("block_type") == "course":
                        block_queue.put(block)
                # fill in the children field
                while not block_queue.empty():
                    item = block_queue.get()
                    fields = item.get("fields")
                    children = fields.get("children")
                    if fields and children:
                        new_children = []
                        for child in children:
                            new_children.append(blocks_dict[child[1]])
                            block_queue.put(blocks_dict[child[1]])
                            blocks.remove(blocks_dict.get(child[1]))
                            # blocks_to_remove.add(child[1])
                        item["fields"]["children"] = new_children
            raw_data[RD_COURSE_IN_MONGO] = courseid_to_structure
            raw_data[RD_DB] = MongoDB(DBC.DB_HOST, DBC.DB_NAME)
        return raw_data

class ParseCourseStructFile(PipeModule):
    '''Parse the mongodb to get course info and video info
    '''
    order = 1
    YOUTUBE_URL_PREFIX = 'https://www.youtube.com/watch?v='

    def __init__(self):
        super().__init__()
        self.re_ISO_8601_duration = re.compile(
            r"^(?P<sign>[+-])?"
            r"P(?!\b)"
            r"(?P<years>[0-9]+([,.][0-9]+)?Y)?"
            r"(?P<months>[0-9]+([,.][0-9]+)?M)?"
            r"(?P<weeks>[0-9]+([,.][0-9]+)?W)?"
            r"(?P<days>[0-9]+([,.][0-9]+)?D)?"
            r"((?P<separator>T)(?P<hours>[0-9]+([,.][0-9]+)?H)?"
            r"(?P<minutes>[0-9]+([,.][0-9]+)?M)?"
            r"(?P<seconds>[0-9]+([,.][0-9]+)?S)?)?$"
        )
        # self.course_video_videokey = {}
        # self.course_video_coursekey = {}
        # self.video_duration = {}
        self.videos = None
        self.courses = None
        # self.video_url_duration = {}
        # self.video_id_info = {}
        self.course_overview = None
        # self.edx_videos = None
        # self.edx_course_videos = None
        # self.video_encode = None
        self.course_access_role = None
        self.course_structures = None

        youtube_api_host = 'https://www.googleapis.com/youtube/v3/videos'
        params = 'part=contentDetails&key=' + TPK.Youtube_key
        self.youtube_api = youtube_api_host + '?' + params

    def load_data(self, raw_data):
        '''Load target file
        '''
        self.course_overview = raw_data.get('course_overviews_courseoverview') or []
        # self.edx_videos = raw_data.get('edxval_video') or []
        # self.edx_course_videos = raw_data.get('edxval_coursevideo') or []
        # self.video_encode = raw_data.get('edxval_encodedvideo') or []
        self.course_access_role = raw_data.get('student_courseaccessrole') or []
        self.course_structures = raw_data.get(RD_COURSE_IN_MONGO) or {}
        database = raw_data[RD_DB]
        video_collection = database.get_collection(DBC.COLLECTION_VIDEO).find({})
        course_collection = database.get_collection(DBC.COLLECTION_COURSE).find({})
        self.videos = {video[DBC.FIELD_VIDEO_ORIGINAL_ID]:video for video in video_collection}
        self.courses = {course[DBC.FIELD_COURSE_ORIGINAL_ID]:course for course in course_collection}



    def parse_video_duration(self, datestring):
        '''Parse the duration of youtube video to human readable timestamp
        '''
        if not isinstance(datestring, str):
            raise TypeError("Expecting a string %r" % datestring)
        match = self.re_ISO_8601_duration.match(datestring)
        if not match:
            raise BaseException("Unable to parse duration string %r" % datestring)
        groups = match.groupdict()
        for key, val in groups.items():
            if key not in ('separator', 'sign'):
                if val is None:
                    groups[key] = "0n"
                groups[key] = float(groups[key][:-1].replace(',', '.'))
        if groups["years"] == 0 and groups["months"] == 0:
            ret = timedelta(days=groups["days"], hours=groups["hours"],
                            minutes=groups["minutes"], seconds=groups["seconds"],
                            weeks=groups["weeks"])
            if groups["sign"] == '-':
                ret = timedelta(0) - ret
        else:
            raise BaseException("there must be something woring in this time string")
        return ret

    def fetch_video_duration(self, url):
        '''Parse the video duration from the url
        '''
        header = {"Range": "bytes=0-100"}
        try:
            result = http_get(url, header)
        except urllib.error.URLError as ex:
            warn("Parse video:"+url+"duration failed. It is probably because ssl and certificate\
                 problem")
            warn(ex)
            return -1
        if result and (result.get_return_code() < 200 or result.get_return_code() >= 300):
            return -1
        video_length = -1
        try:
            bio = io.BytesIO(result.get_content())
            data = bio.read(8)
            al, an = struct.unpack('>I4s', data)
            an = an.decode()
            assert an == 'ftyp'
            bio.read(al - 8)
            data = bio.read(8)
            al, an = struct.unpack('>I4s', data)
            an = an.decode()
            assert an == 'moov'
            data = bio.read(8)
            al, an = struct.unpack('>I4s', data)
            an = an.decode()
            assert an == 'mvhd'
            data = bio.read(20)
            infos = struct.unpack('>12x2I', data)
            video_length = int(infos[1]) // int(infos[0])
        except BaseException as ex:
            warn("Parse video:"+ url + "duration failed")
            warn(ex)
        return video_length

    def process(self, raw_data, raw_data_filenames=None):
        info("Processing ParseCourseStructFile")

        self.load_data(raw_data)
        course_instructors = {}
        tmp_youtube_video_dict = {}
        tmp_other_video_dict = {}
        # the db file stay unchanged
        if self.course_overview is None:
            processed_data = raw_data
            processed_data['data'][DBC.COLLECTION_VIDEO] = self.videos
            return processed_data

        for access_role in self.course_access_role:
            records = split(access_role)
            if len(records) < 2:
                continue
            if records[3] != 'instructor' and records[3] != "staff":
                continue
            course_id = records[2]
            try:
                course_id = course_id[course_id.index(':') + 1:]
            except ValueError as ex:
                warn("In ParseCourseStructFile, cannot get courseId when try to get access role of\
                     course:"+course_id)
                warn(ex)
                continue
            course_id = course_id.replace('.', '_')
            user_id = records[4]
            course_instructors.setdefault(course_id, []).append(user_id)

        course_year = "course_year"
        course_year_pattern = r'^course-[\w|:|\+]+(?P<' + course_year + r'>[0-9]{4})\w*'
        re_course_year = re.compile(course_year_pattern)

        for course_item in self.course_overview:
            try:
                records = split(course_item)
                course_original_id = records[3]
                try:
                    course_original_id = course_original_id[course_original_id.index(':') + 1:]
                except ValueError as ex:
                    warn("In ParseCourseStructFile, cannot get courseId when try to get originalId\
                         of course:"+course_original_id)
                    warn(ex)
                    continue
                course_original_id = course_original_id.replace('.', '_')
                course = self.courses.get(course_original_id) or {}
                course_start_time = try_parse_date(records[8])
                course_end_time = try_parse_date(records[9])
                advertised_start_time = try_parse_date(records[10])
                enrollment_start_time = try_parse_date(records[26])
                enrollment_end_time = try_parse_date(records[27])
                # construct the course object
                course[DBC.FIELD_COURSE_ORIGINAL_ID] = course_original_id
                course[DBC.FIELD_COURSE_NAME] = records[5]
                course_year_match = re_course_year.search(course_original_id)
                course[DBC.FIELD_COURSE_YEAR] = course_year_match and \
                    course_year_match.group(course_year)
                course[DBC.FIELD_COURSE_INSTRUCTOR] = course_instructors.get(
                    course_original_id) or []
                course[DBC.FIELD_COURSE_STATUS] = None
                course[DBC.FIELD_COURSE_URL] = None
                course[DBC.FIELD_COURSE_IMAGE_URL] = records[11]
                course[DBC.FIELD_COURSE_DESCRIPTION] = records[35]
                # a set of timestamp
                course[DBC.FIELD_COURSE_STARTTIME] = course_start_time
                course[DBC.FIELD_COURSE_ENDTIME] = course_end_time
                course[DBC.FIELD_COURSE_ENROLLMENT_START] = enrollment_start_time
                course[DBC.FIELD_COURSE_ENROLLMENT_END] = enrollment_end_time
                course[DBC.FIELD_COURSE_ADVERTISED_START] = advertised_start_time

                course[DBC.FIELD_COURSE_STUDENT_IDS] = set()
                course[DBC.FIELD_COURSE_METAINFO] = None
                course[DBC.FIELD_COURSE_ORG] = records[36]
                course[DBC.FIELD_COURSE_LOWEST_PASSING_GRADE] = records[21]
                course[DBC.FIELD_COURSE_MOBILE_AVAILABLE] = records[23]
                course[DBC.FIELD_COURSE_DISPLAY_NUMBER_WITH_DEFAULT] = records[6]
                course_structure = self.course_structures.get(course_original_id)
                if course_structure:
                    for block in course_structure.get('blocks'):
                        if block.get('block_type') == 'course':
                            for chapter in block['fields']['children']:
                                for sequential in chapter['fields']['children']:
                                    for vertical in sequential['fields']['children']:
                                        for leaf in vertical['fields']['children']:
                                            if leaf.get('block_type') == 'video' and leaf.get("fields"):
                                                fields = leaf.get('fields')
                                                video_original_id = leaf.get('block_id')
                                                video = {}
                                                video[DBC.FIELD_VIDEO_ORIGINAL_ID] = video_original_id
                                                video[DBC.FIELD_VIDEO_NAME] = fields.get(
                                                    'display_name')
                                                video[DBC.FIELD_VIDEO_URL] = (fields.get('youtube_id_1_0') and ParseCourseStructFile.YOUTUBE_URL_PREFIX + fields.get('youtube_id_1_0')) or \
                                                    (fields.get('html5_sources')
                                                     and fields.get('html5_sources')[0])
                                                video[DBC.FIELD_VIDEO_TEMPORAL_HOTNESS] = {}
                                                # this code seems is uselessfulle
                                                # video[DBC.FIELD_VIDEO_DURATION] = self.video_url_duration.get(
                                                #   video[DBC.FIELD_VIDEO_URL])
                                                video[DBC.FIELD_VIDEO_DESCRIPTION] = fields.get(
                                                    'display_name')
                                                video[DBC.FIELD_VIDEO_SECTION] = chapter['fields']['display_name'] + ', ' + \
                                                    sequential['fields']['display_name'] + \
                                                    ', ' + vertical['fields']['display_name']
                                                video[DBC.FIELD_VIDEO_COURSE_ID] = course_original_id

                                                # if the url is unchange, use the old duration in db
                                                new_url = video.get(DBC.FIELD_VIDEO_URL)
                                                if video_original_id in self.videos:
                                                    old_video = self.videos[video_original_id]
                                                    old_url = old_video.get(DBC.FIELD_VIDEO_URL)
                                                    if  old_url == new_url:
                                                        video[DBC.FIELD_VIDEO_DURATION] = old_video.get(DBC.FIELD_VIDEO_DURATION)
                                                # else if the url is from youtube
                                                elif new_url and 'youtube' in new_url:
                                                    youtube_id = new_url[new_url.index('v=') + 2:]
                                                    tmp_youtube_video_dict[
                                                        youtube_id] = video_original_id
                                                # else if the url is from other website
                                                elif new_url:
                                                    tmp_other_video_dict.setdefault(
                                                        new_url, []).append(video_original_id)
                                                self.videos[video_original_id] = video
                                                course.setdefault(DBC.FIELD_COURSE_VIDEO_IDS, []).append(
                                                    video_original_id)
                self.courses[course_original_id] = course
                if course_original_id == "HKUST+bt001+2016_Q1_R1":
                    print(str(course))
            except BaseException as ex:
                warn("In ParseCourseStructFile, cannot get the course information of course:"\
                     +str(course_item))
                warn(ex)

        # fetch the video duration from youtube_api_v3
        urls = [self.youtube_api + '&id=' +
                youtube_id for youtube_id in tmp_youtube_video_dict.keys()]
        broken_youtube_id = set(tmp_youtube_video_dict.keys())
        results = http_get_list(urls, limit=60)
        for result in results:
            result = json.loads(str(result, 'utf-8'))
            items = result.get("items")
            if items is None or len(items) <= 0:
                continue
            video_id = items[0].get("id")
            broken_youtube_id.discard(video_id)
            duration = self.parse_video_duration(items[0]["contentDetails"]["duration"])
            self.videos[tmp_youtube_video_dict[video_id]][
                DBC.FIELD_VIDEO_DURATION] = int(duration.total_seconds())
        # fetch the video duration from other websites
        for url in tmp_other_video_dict:
            video_duration = self.fetch_video_duration(url)
            video_ids = tmp_other_video_dict[url]
            for video_id in video_ids:
                self.videos[video_id][DBC.FIELD_VIDEO_DURATION] = video_duration

        processed_data = raw_data
        processed_data[RD_DATA][DBC.COLLECTION_VIDEO] = self.videos
        processed_data[RD_DATA][DBC.COLLECTION_COURSE] = self.courses
        return processed_data

class ParseUserFile(PipeModule):
    '''Parse user information from db snapshot
    '''
    order = 2

    def __init__(self):
        super().__init__()
        self.users = None
        self.user_profile = {}
        self.user_roles = {}
        self.raw_user_profile = None
        self.user_info = None
        self.course_access_role = None

    def load_data(self, raw_data):
        '''Load target file
        '''
        self.raw_user_profile = raw_data.get('auth_userprofile') or []
        self.user_info = raw_data.get('auth_user')
        self.course_access_role = raw_data.get('student_courseaccessrole') or []
        database = raw_data[RD_DB]
        user_collection = database.get_collection(DBC.COLLECTION_USER).find({})
        self.users = {user[DBC.FIELD_VIDEO_ORIGINAL_ID]: user for user in user_collection}

    def process(self, raw_data, raw_data_filenames=None):
        info("Processing ParseUserFile")
        self.load_data(raw_data)
        for record in self.raw_user_profile:
            fields = split(record)
            self.user_profile[fields[16]] = fields
        if self.user_info is None:
            return raw_data

        for access_role in self.course_access_role:
            records = split(access_role)
            if len(records) < 3:
                continue
            course_id = records[2]
            try:
                course_id = course_id[course_id.index(':') + 1:]
            except ValueError as ex:
                warn("In ParseUserFile, cannot get courseId when try to get access role of\
                     course:"+access_role)
                warn(ex)
            course_id = course_id.replace('.', '_')
            self.user_roles.setdefault(records[4], {}).setdefault(course_id, []).append(records[3])

        for record in self.user_info:
            try:
                user_fields = split(record)
                user_id = user_fields[0]
                user = self.users.get(user_id) or {}
                user_profile = self.user_profile.get(user_id)
                birth_year = datetime.strptime(user_profile[6], '%Y')\
                    if (user_profile and (user_profile[6] != "NULL" and
                                          len(user_profile[6]) == 4)) else None
                user[DBC.FIELD_USER_USER_NAME] = user_fields[4]
                user[DBC.FIELD_USER_LANGUAGE] = user_profile and user_profile[4]
                user[DBC.FIELD_USER_LOCATION] = user_profile and user_profile[5]
                user[DBC.FIELD_USER_BIRTH_DATE] = birth_year and birth_year.year
                user[DBC.FIELD_USER_EDUCATION_LEVEL] = user_profile and user_profile[8]
                user[DBC.FIELD_USER_GENDER] = user_profile and user_profile[7]
                user[DBC.FIELD_USER_COURSE_IDS] = set()
                user[DBC.FIELD_USER_DROPPED_COURSE_IDS] = set()
                user[DBC.FIELD_USER_BIO] = user_profile and user_profile[14]
                user[DBC.FIELD_USER_COUNTRY] = user_profile and (
                    user_profile[11] or user_profile[5])
                user[DBC.FIELD_USER_NAME] = user_fields[5] + user_fields[6]
                user[DBC.FIELD_USER_ORIGINAL_ID] = user_id
                user[DBC.FIELD_USER_COURSE_ROLE] = self.user_roles.get(user_id) or {}
                self.users[user_id] = user
            except BaseException as ex:
                warn("In ParseUserFile, cannot get the user information of record:"+record+", and\
                     userProfile:"+str(user_profile))
                warn(ex)

        processed_data = raw_data
        # user collection needs courseIds and droppedCourseIds
        processed_data['data'][DBC.COLLECTION_USER] = self.users
        return processed_data

class ParseEnrollmentFile(PipeModule):
    '''Parse enrollment file
    '''
    order = 3
    ENROLL = "enroll"
    UNENROLL = "unenroll"
    action = {'1': ENROLL, '0': UNENROLL}

    def __init__(self):
        super().__init__()
        self.course_enrollment = None

    def load_data(self, raw_data):
        '''Load target file
        '''
        self.course_enrollment = raw_data.get('student_courseenrollment') or []

    def process(self, raw_data, raw_data_filenames=None):
        info("Processing ParseEnrollmentFile")
        self.load_data(raw_data)
        if self.course_enrollment is None:
            return raw_data
        courses = raw_data[RD_DATA][DBC.COLLECTION_COURSE]
        users = raw_data[RD_DATA][DBC.COLLECTION_USER]

        enrollments = []
        for enroll_item in self.course_enrollment:
            try:
                enrollment = {}
                records = split(enroll_item)
                user_id = records[5]
                course_id = records[1]
                course_id = course_id[course_id.index(':') + 1:]
                course_id = course_id.replace('.', '_')
                enrollment_time = try_parse_date(records[2])
                enrollment[DBC.FIELD_ENROLLMENT_USER_ID] = user_id
                enrollment[DBC.FIELD_ENROLLMENT_COURSE_ID] = course_id
                enrollment[DBC.FIELD_ENROLLMENT_TIMESTAMP] = enrollment_time
                enrollment[DBC.FIELD_ENROLLMENT_ACTION] = ParseEnrollmentFile.action.get(records[3])
                enrollments.append(enrollment)
                # fill in user collection
                if enrollment[DBC.FIELD_ENROLLMENT_ACTION] == ParseEnrollmentFile.ENROLL:
                    users[user_id][DBC.FIELD_USER_COURSE_IDS].add(course_id)
                    users[user_id][DBC.FIELD_USER_DROPPED_COURSE_IDS].discard(course_id)
                    courses[course_id][DBC.FIELD_COURSE_STUDENT_IDS].add(user_id)
                elif enrollment[DBC.FIELD_ENROLLMENT_ACTION] == ParseEnrollmentFile.UNENROLL:
                    users[user_id][DBC.FIELD_USER_DROPPED_COURSE_IDS].add(course_id)
                    users[user_id][DBC.FIELD_USER_COURSE_IDS].discard(course_id)
                    courses[course_id][DBC.FIELD_COURSE_STUDENT_IDS].discard(user_id)
            except ValueError as ex:
                warn("In ParseEnrollmentFile, cannot get the enrollment information of item:"+\
                     enroll_item)
                warn(ex)
            # except BaseException as ex:
            #     warn(ex)
            #     warn("enrollment userId " + user_id + ", courseId " + course_id)

        processed_data = raw_data
        # course and users collection are completed
        processed_data['data'][DBC.COLLECTION_ENROLLMENT] = enrollments
        # processed_data['data'][DBC.COLLECTION_USER] = list(users.values())
        # processed_data['data'][DBC.COLLECTION_COURSE] = list(courses.value())
        return processed_data

class ParseLogFile(PipeModule):
    '''Parse log files
    '''
    order = 4

    def __init__(self):
        super().__init__()

    def load_data(self, data_filenames):
        '''Load target file
        '''
        for filename in data_filenames:
            if FC.Clickstream_suffix in filename:
                with open(filename, 'r', encoding='utf-8') as file:
                    raw_data = file.readlines()
                    yield raw_data

    def process(self, raw_data, raw_data_filenames=None):
        info("Processing log files")
        all_data_to_be_processed = self.load_data(raw_data_filenames)

        if all_data_to_be_processed is None:
            return raw_data

        pattern_wrong_username = r'"username"\s*:\s*"",'
        pattern_right_eventsource = r'"event_source"\s*:\s*"browser"'
        pattern_right_eventtype = r'"event_type"\s*:\s*"(hide_transcript|load_video|' + \
            r'pause_video|play_video|seek_video|show_transcript|speed_change_video|stop_video|' + \
            r'video_hide_cc_menu|video_show_cc_menu)"'

        pattern_context = r',?\s*("context"\s*:\s*{[^}]*})'
        pattern_event = r',?\s*("event"\s*:\s*"([^"]|\\")*(?<!\\)")'
        pattern_username = r',?\s*("username"\s*:\s*"[^"]*")'
        pattern_time = r',?\s*("time"\s*:\s*"[^"]*")'
        pattern_event_json_escape_ = r'"(?={)|"$'

        re_wrong_username = re.compile(pattern_wrong_username)
        re_right_eventsource = re.compile(pattern_right_eventsource)
        re_right_eventtype = re.compile(pattern_right_eventtype)
        re_context = re.compile(pattern_context)
        re_event = re.compile(pattern_event)
        re_username = re.compile(pattern_username)
        re_time = re.compile(pattern_time)
        re_event_json_escape = re.compile(pattern_event_json_escape_)
        temp_video_dict = raw_data['data'][DBC.COLLECTION_VIDEO]

        events = []
        denselogs = {}
        pattern_time = "%Y-%m-%dT%H:%M:%S.%f+00:00"
        count = 0
        for data_to_be_processed in all_data_to_be_processed:
            count += 1
            info("This is " + str(count) + "th log file")
            for line in data_to_be_processed:
                try:
                    event_type = re_right_eventtype.search(line)
                    if re_wrong_username.search(line) is None and \
                            re_right_eventsource.search(line) is not None and \
                            event_type is not None:
                        context = re_context.search(line)
                        event_field = re_event.search(line)
                        username = re_username.search(line)
                        timestamp = re_time.search(line)
                        temp_data = [event_type.group()]
                        if context is not None:
                            temp_data.append(context.group(1))
                        if event_field is not None:
                            temp_data.append(re_event_json_escape.sub(
                                '', event_field.group(1).replace('\\', '')))
                        if username is not None:
                            temp_data.append(username.group(1))
                        if timestamp is not None:
                            temp_data.append(timestamp.group(1))
                        str_temp_data = "{" + ",".join(temp_data) + "}"
                        str_temp_data = str_temp_data.replace('.,', ',', 1)
                        temp_data = json.loads(str_temp_data)

                        event = {}
                        event_context = temp_data.get('context') or {}
                        event_event = temp_data.get('event') or {}
                        video_id = event_event.get('id')
                        str_event_time = temp_data.get('time')
                        if '.' not in str_event_time:
                            try:
                                str_event_time = str_event_time[:str_event_time.index("+")] + \
                                    '.000000' + str_event_time[str_event_time.index("+"):]
                            except ValueError as ex:
                                warn("In ParseLogFile, cannot process the event_time:"+\
                                     str_event_time)
                                warn(ex)

                        course_id = event_context.get('course_id')
                        try:
                            course_id = course_id[course_id.index(':') + 1:]
                        except ValueError as ex:
                            warn("In ParseLogFile, cannot get courseId of:"+course_id)
                            warn(ex)
                            continue
                        course_id = course_id.replace('.', '_')
                        target_attrs = {'path':'path', 'code':'code', 'currentTime':'currentTime',\
                            'new_time':'newTime', 'old_time':'oldTime', 'new_speed':'newSpeed',\
                            'old_speed':'oldSpeed'}
                        event_time = datetime.strptime(str_event_time, pattern_time)
                        event[DBC.FIELD_VIDEO_LOG_USER_ID] = event_context.get('user_id')
                        event[DBC.FIELD_VIDEO_LOG_VIDEO_ID] = video_id
                        event[DBC.FIELD_VIDEO_LOG_COURSE_ID] = course_id
                        event[DBC.FIELD_VIDEO_LOG_TIMESTAMP] = event_time.timestamp()
                        event[DBC.FIELD_VIDEO_LOG_TYPE] = temp_data.get('event_type')
                        event[DBC.FIELD_VIDEO_LOG_METAINFO] = {target_attrs[k]: event_event.get(
                            k) for k in target_attrs if event_event.get(k) is not None}
                        event[DBC.FIELD_VIDEO_LOG_METAINFO]['path'] = event_context.get('path')
                        events.append(event)

                        # ready to denselogs
                        denselog_time = datetime(event_time.year, event_time.month,
                                                 event_time.day).timestamp()
                        denselogs_key = (video_id + str(denselog_time)) if video_id else \
                            "none_video_id" + str(denselog_time)
                        if denselogs.get(denselogs_key) is None:
                            denselog = denselogs[denselogs_key] = {}
                            denselog[DBC.FIELD_VIDEO_DENSELOGS_COURSE_ID] = course_id
                            denselog[DBC.FIELD_VIDEO_DENSELOGS_TIMESTAMP] = denselog_time
                            denselog[DBC.FIELD_VIDEO_DENSELOGS_VIDEO_ID] = video_id
                        click = {}
                        click[DBC.FIELD_VIDEO_DENSELOGS_ORIGINAL_ID] = ''
                        click[DBC.FIELD_VIDEO_DENSELOGS_USER_ID] = event_context.get('user_id')
                        click[DBC.FIELD_VIDEO_DENSELOGS_TYPE] = temp_data.get('event_type')
                        click[DBC.FIELD_VIDEO_DENSELOGS_PATH] = event_context.get('path')
                        click.update(event[DBC.FIELD_VIDEO_LOG_METAINFO])
                        denselogs[denselogs_key].setdefault(
                            DBC.FIELD_VIDEO_DENSELOGS_CLICKS, []).append(click)

                        if temp_video_dict.get(video_id):
                            temporal_hotness = temp_video_dict[video_id][
                                DBC.FIELD_VIDEO_TEMPORAL_HOTNESS]
                            date_time = str(event_time.date())
                            if date_time not in temporal_hotness:
                                temporal_hotness[date_time] = 0
                            temporal_hotness[date_time] += 1
                except BaseException as ex:
                    warn("In ParseLogFile, some problem happend:"+line)
                    warn(ex)

        processed_data = raw_data
        processed_data['data'][DBC.COLLECTION_VIDEO_LOG] = events
        processed_data['data'][DBC.COLLECTION_VIDEO_DENSELOGS] = list(denselogs.values())
        return processed_data

class DumpToDB(PipeModule):
    '''Dump the processed data into database
    '''
    order = 998

    def __init__(self):
        super().__init__()
        self.need_drop_collections = [DBC.COLLECTION_COURSE, DBC.COLLECTION_ENROLLMENT,
                                      DBC.COLLECTION_USER, DBC.COLLECTION_VIDEO]

    def process(self, raw_data, raw_data_filenames=None):
        info("Insert data to DB")
        db_data = raw_data['data']
        # cast from set to list
        courses = db_data.get(DBC.COLLECTION_COURSE)
        users = db_data.get(DBC.COLLECTION_USER)
        if courses:
            for course_info in courses.values():
                course_info[DBC.FIELD_COURSE_STUDENT_IDS] = list(
                    course_info[DBC.FIELD_COURSE_STUDENT_IDS])
        if users:
            for user in users.values():
                user[DBC.FIELD_USER_COURSE_IDS] = list(user[DBC.FIELD_USER_COURSE_IDS])
                user[DBC.FIELD_USER_DROPPED_COURSE_IDS] = list(
                    user[DBC.FIELD_USER_DROPPED_COURSE_IDS])
        # from dictory to list, removing id index
        for (key, value) in db_data.items():
            if isinstance(value, dict):
                db_data[key] = list(value.values())

        # insert to db
        database = raw_data.get(RD_DB)
        for collection_name in db_data:
            collection = database.get_collection(collection_name)
            if collection_name in self.need_drop_collections:
                collection.delete_many({})
            if db_data[collection_name] and len(db_data[collection_name]) > 0:
                collection.insert_many(db_data[collection_name])
        return raw_data

# class SetEncoder(json.JSONEncoder):
#     # pylint: disable=E0202

#     def default(self, obj):

#         if type(obj) is set:
#             return list(obj)
#         elif isinstance(obj, ObjectId):
#             return str(obj)
#         return json.JSONEncoder.default(self, obj)
