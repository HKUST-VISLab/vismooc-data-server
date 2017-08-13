'''Util functions for processing data
'''
import hashlib
import io
import multiprocessing
import re
import struct
import urllib
from datetime import timedelta, datetime

import pymysql

from mathematician.config import DBConfig as DBc
from mathematician.DB.mongo_dbhelper import MongoDB
from mathematician.http_helper import get as http_get
from mathematician.logger import warn

PARALLEL_GRAIN = 20

RE_ISO_8601 = re.compile(
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

def try_get_timestamp(date):
    '''Try to get timestamp from a date object
    '''
    return date and str(round(date.timestamp() * 1000))  # in milliseconds

def try_get_date(timestamp):
    '''Try to get date object from timestamp
    '''
    if isinstance(timestamp, int) or isinstance(timestamp, float):
        timestamp = str(timestamp)
    return datetime.fromtimestamp(timestamp)


def try_parse_date(date_str, pattern="%Y-%m-%d %H:%M:%S.%f"):
    '''Try to parse a date string and get the timestamp
        If can not parse the string based on certain pattern, return None
    '''
    try:
        return datetime.strptime(date_str, pattern)
    except ValueError:
        return None

def try_parse_course_id(course_id):
    '''Try parse course id to form an uniform format
    '''
    try:
        if ':' in course_id:
            course_id = course_id[course_id.index(':')+1:]
        course_id = re.sub(r'[\.|\/|\+]', '_', course_id)
    except ValueError as err:
        raise err
    return course_id

DAY_TS = 1000 * 60 * 60 * 24
def round_timestamp_to_day(timestamp):
    '''Round the timestamp from ms to day
    '''
    if isinstance(timestamp, datetime):
        timestamp = try_get_timestamp(timestamp)
    if isinstance(timestamp, str):
        timestamp = int(timestamp)
    try:
        return str(round(timestamp / DAY_TS) * DAY_TS)
    except BaseException as err:
        warn('Err in round_timestamp_to_day')
        warn(err)
    # raise ValueError("The timestamp should be int or float type")

def get_data_by_table(tablename):
    ''' Get all the data from a table
    '''
    if DBc.SQL_CONFIG is None:
        return
    sql_db = pymysql.connect(**DBc.SQL_CONFIG)
    cursor = sql_db.cursor()
    cursor.execute("SELECT * FROM " + tablename)
    results = cursor.fetchall()
    sql_db.close()
    return results

def fetch_video_duration(url):
    '''fetch the video duration from the url
    '''
    header = {"Range": "bytes=0-100"}
    try:
        result = http_get(url, header)
    except urllib.error.URLError as ex:
        warn("Parse video:" + url + "duration failed. It is probably because ssl and certificate problem")
        warn(ex)
        return -1
    if result and (result.get_return_code() < 200 or result.get_return_code() >= 300):
        return -1
    video_duration = -1
    try:
        bio = io.BytesIO(result.get_content())
        data = bio.read(8)
        code, field = struct.unpack('>I4s', data)
        field = field.decode()
        assert field == 'ftyp'
        bio.read(code - 8)
        data = bio.read(8)
        code, field = struct.unpack('>I4s', data)
        field = field.decode()
        assert field == 'moov'
        data = bio.read(8)
        code, field = struct.unpack('>I4s', data)
        field = field.decode()
        assert field == 'mvhd'
        data = bio.read(20)
        infos = struct.unpack('>12x2I', data)
        video_duration = int(infos[1]) // int(infos[0])
    except BaseException as ex:
        warn("Parse video:" + url + "duration failed")
        warn(ex)
    return video_duration

def parse_duration_from_youtube_api(datestring):
    '''Parse video duration from the result return from youtube api
    '''
    if not isinstance(datestring, str):
        raise TypeError("Expecting a string %r" % datestring)
    match = RE_ISO_8601.match(datestring)
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
        raise BaseException(
            "there must be something woring in this time string")
    return ret

def get_cpu_num():
    '''Get the cpu number of the machine
    '''
    cpu_num = multiprocessing.cpu_count()
    cpu_num = cpu_num - 1 if cpu_num > 1 else cpu_num
    return cpu_num

def is_processed(filename):
    ''' Check whether a file is processed or not according to
        the metadbfiles collection records
    '''
    database = MongoDB(DBc.DB_NAME, DBc.DB_HOST, DBc.DB_PORT)
    metadbfile = database.get_collection(DBc.COLLECTION_METADBFILES)
    md5 = hashlib.md5()
    with open(filename, 'r', encoding='utf-8') as file:
        md5.update(file.read().encode('utf-8'))
    digest = md5.hexdigest()
    db_entry = metadbfile.find_one({
        DBc.FIELD_METADBFILES_ETAG: digest, DBc.FIELD_METADBFILES_PROCESSED: True
    })
    return True if db_entry else False

def try_parse_video_id(video_id):
    '''Try parse video id to form an uniform format
    '''
    try:
        if '@' in video_id:
            video_id = video_id.split('@')[-1]
    except ValueError as err:
        raise err
    return video_id
