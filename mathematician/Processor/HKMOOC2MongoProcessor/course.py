import json

from mathematician.config import DBConfig as DBc
from mathematician.http_helper import get_list as http_get_list
from mathematician.logger import info, warn
from mathematician.pipe import PipeModule
from mathematician.config import ThirdPartyKeys as TPKc
from mathematician.Processor.utils import (fetch_video_duration,
                                           parse_duration_from_youtube_api,
                                           split, try_get_timestamp)

from .constant import RD_COURSE_IN_MONGO, RD_DATA, RD_DB


class CourseProcessor(PipeModule):
    '''Parse the mongodb to get course info and video info
    '''
    order = 1
    YOUTUBE_URL_PREFIX = 'https://www.youtube.com/watch?v='

    def __init__(self, offline=False):
        super().__init__()
        self.offline = offline
        self.videos = None
        self.courses = None
        self.course_overview = None
        self.course_access_role = None
        self.course_structures = None

        youtube_api_host = 'https://www.googleapis.com/youtube/v3/videos'
        params = 'part=contentDetails&key=' + TPKc.Youtube_key
        self.youtube_api = youtube_api_host + '?' + params

    def load_data(self, raw_data):
        '''Load target file
        '''
        self.course_overview = raw_data.get('course_overviews_courseoverview') or []
        self.course_access_role = raw_data.get('student_courseaccessrole') or []
        self.course_structures = raw_data.get(RD_COURSE_IN_MONGO)
        database = raw_data[RD_DB]
        video_collection = database.get_collection(DBc.COLLECTION_VIDEO).find({})
        self.videos = {video[DBc.FIELD_VIDEO_ORIGINAL_ID]:video for video in video_collection}

        course_collection = database.get_collection(DBc.COLLECTION_COURSE).find({})
        self.courses = {course[DBc.FIELD_COURSE_ORIGINAL_ID]:course for course in course_collection}
        for course in self.courses.values():
            course[DBc.FIELD_COURSE_STUDENT_IDS] = set(course[DBc.FIELD_COURSE_STUDENT_IDS])
            course[DBc.FIELD_COURSE_VIDEO_IDS] = set(course[DBc.FIELD_COURSE_VIDEO_IDS])

    def construct_video(self, video_block):
        '''Construct a video object based on the mongodb snapshot
        '''
        fields = video_block.get('fields')
        video_original_id = str(video_block.get('block_id'))
        video = self.videos.get(video_original_id) or {}
        video[DBc.FIELD_VIDEO_ORIGINAL_ID] = video_original_id
        video_title = str(fields.get('display_name'))
        video[DBc.FIELD_VIDEO_NAME] = video_title
        video[DBc.FIELD_VIDEO_TEMPORAL_HOTNESS] = video.get(
            DBc.FIELD_VIDEO_TEMPORAL_HOTNESS) or {}
        video[DBc.FIELD_VIDEO_DESCRIPTION] = video_title
        video[DBc.FIELD_VIDEO_SECTION] = video_block.get('prefix')
        return video

    def process(self, raw_data, raw_data_filenames=None):
        '''process the data
        '''
        info("Processing ParseCourseStructFile")

        self.load_data(raw_data)
        course_instructors = {}
        tmp_youtube_video_dict = {}
        tmp_other_video_dict = {}
        # the db file stay unchanged
        if self.course_overview is None:
            processed_data = raw_data
            processed_data['data'][DBc.COLLECTION_VIDEO] = self.videos
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
                     course:" + course_id)
                warn(ex)
                continue
            course_id = course_id.replace('.', '_')
            user_id = records[4]
            course_instructors.setdefault(course_id, []).append(user_id)

        if len(self.course_overview) == 0:
            warn("No course_overviews_courseoverview in MySQL snapshots")
        for course_item in self.course_overview:
            try:
                records = split(course_item)
                course_original_id = records[3]
                try:
                    course_original_id = course_original_id[
                        course_original_id.index(':') + 1:]
                except ValueError as ex:
                    warn("In ParseCourseStructFile, cannot get courseId when try to get originalId\
                         of course:" + course_original_id)
                    warn(ex)
                    continue
                course_original_id = course_original_id.replace('.', '_')
                course = self.courses.get(course_original_id) or {}
                # construct the course object
                # init the video id list
                course[DBc.FIELD_COURSE_VIDEO_IDS] = set()
                course[DBc.FIELD_COURSE_ORIGINAL_ID] = course_original_id
                course[DBc.FIELD_COURSE_NAME] = records[5]
                course[DBc.FIELD_COURSE_YEAR] = course_original_id.split('+').pop()
                course[DBc.FIELD_COURSE_INSTRUCTOR] = course_instructors.get(
                    course_original_id) or []
                course[DBc.FIELD_COURSE_STATUS] = None
                course[DBc.FIELD_COURSE_URL] = None
                course[DBc.FIELD_COURSE_IMAGE_URL] = records[11]
                course[DBc.FIELD_COURSE_DESCRIPTION] = records[34]

                course[DBc.FIELD_COURSE_STUDENT_IDS] = set()
                course[DBc.FIELD_COURSE_METAINFO] = None
                course[DBc.FIELD_COURSE_ORG] = records[35]
                course_block = self.course_structures.get(course_original_id)

                # pylint: disable=C0301
                if course_block and course_block.get('fields'):
                    block_field = course_block.get('fields')
                    course[DBc.FIELD_COURSE_STARTTIME] = try_get_timestamp(
                        block_field.get('start'))
                    course[DBc.FIELD_COURSE_ENDTIME] = try_get_timestamp(
                        block_field.get('end'))
                    course[DBc.FIELD_COURSE_ENROLLMENT_START] = try_get_timestamp(block_field.get('enrollment_start'))\
                        or course[DBc.FIELD_COURSE_STARTTIME]
                    course[DBc.FIELD_COURSE_ENROLLMENT_END] = try_get_timestamp(
                        block_field.get('enrollment_end'))
                    course[DBc.FIELD_COURSE_NAME] = block_field.get('display_name')

                    course_children = course_block.get("children")
                    if course_children:
                        for child in course_children:
                            child_fields = child.get('fields')
                            if child.get('block_type') != 'video' or not child_fields:
                                continue
                            try:
                                video = self.construct_video(child)
                            except BaseException as ex:
                                warn("In CourseProcessor, cannot get the video information of video:" + str(child))
                                warn(ex)
                                continue
                            youtube_id = child_fields.get('youtube_id_1_0')
                            video_original_id = video[
                                DBc.FIELD_VIDEO_ORIGINAL_ID]
                            new_url = (youtube_id and CourseProcessor.YOUTUBE_URL_PREFIX + youtube_id) or (
                                child_fields.get('html5_sources') and child_fields.get('html5_sources')[0])
                            old_url = video.get(DBc.FIELD_VIDEO_URL)
                            if new_url != old_url:
                                video[DBc.FIELD_VIDEO_URL] = new_url
                                # if the url is from youtube
                                if new_url and 'youtube' in new_url:
                                    youtube_id = new_url[new_url.index('v=') + 2:]
                                    tmp_youtube_video_dict[youtube_id] = video_original_id
                                # else if the url is from other website
                                elif new_url:
                                    tmp_other_video_dict.setdefault(
                                        new_url, []).append(video_original_id)
                            self.videos[video_original_id] = video
                            course.setdefault(DBc.FIELD_COURSE_VIDEO_IDS, set()).add(
                                video_original_id)
                else:
                    warn("Course " + course_original_id + " has no course block,"
                         "which means it will has no videos!")
                self.courses[course_original_id] = course
            except BaseException as ex:
                warn("In ParseCourseStructFile, cannot get the course information of course:"
                     + str(course_item))
                warn(ex)
        if self.offline is False:
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
                duration = parse_duration_from_youtube_api(items[0]["contentDetails"]["duration"])
                self.videos[tmp_youtube_video_dict[video_id]][
                    DBc.FIELD_VIDEO_DURATION] = int(duration.total_seconds())
            # fetch the video duration from other websites
            for url in tmp_other_video_dict:
                video_duration = fetch_video_duration(url)
                video_ids = tmp_other_video_dict[url]
                for video_id in video_ids:
                    self.videos[video_id][
                        DBc.FIELD_VIDEO_DURATION] = video_duration
        if len(self.videos) == 0:
            warn("VIDEO:No video in data!")
        if len(self.courses) == 0:
            warn("COURSE:No course in data!")
        processed_data = raw_data
        processed_data[RD_DATA][DBc.COLLECTION_VIDEO] = self.videos
        processed_data[RD_DATA][DBc.COLLECTION_COURSE] = self.courses
        return processed_data
