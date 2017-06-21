import re
from os.path import isfile

import bson

import queue
from mathematician.config import DBConfig as DBc
from mathematician.config import FilenameConfig as FC
from mathematician.DB.mongo_dbhelper import MongoDB
from mathematician.logger import info, warn
from mathematician.pipe import PipeModule

from .constant import RD_COURSE_IN_MONGO, RD_DB


class RawFileProcessor(PipeModule):
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
        # structureids = set()
        current_db = None
        structureid_to_courseid = {}
        module_structure_filename = None

        filenames = [file.get("path")
                     for file in raw_data_filenames if isfile(file.get("path"))]
        info(filenames)
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
                        # remove first '(' and last ';)\n' pay attention to
                        # '\n'
                        line = line[line.index('(') + 1: -3]
                        records = line.split('),(')
                        table_name = match_table.group("table_name")
                        # print('SQL_table_name:'+table_name)
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
                        # structureids.add(oid)
                        structureid_to_courseid[oid] = record['org'] + '+' + \
                            record['course'].replace('.', '_') + '+' + record['run']
            elif FC.STRUCTURES in filename:
                module_structure_filename = filename
        # modulestore.active_version must be processed before
        # modulestore.structures
        info('Try to process module structure files')
        if module_structure_filename and len(structureid_to_courseid) > 0:
            courseid_to_structure = {}
            with open(module_structure_filename, 'rb') as file:
                records = bson.decode_file_iter(file)
                while True:
                    try:
                        record = next(records)
                    except bson.errors.InvalidBSON as err:
                        warn('In Rawfile processor: failed to decode bson')
                        warn(err)
                    except StopIteration:
                        break
                    oid = str(record.get('_id'))
                    if oid in structureid_to_courseid:
                        courseid_to_structure[structureid_to_courseid[oid]] = record
            info('Finish loading module structure data')
            section_sep = ">>"
            target_block_type = {"course", "chapter", "sequential", "vertical", "video"}
            courses = {}
            if len(courseid_to_structure) == 0:
                warn("There is no course strucutre in mongodb file!")
            for course_id in courseid_to_structure:
                info("Process course_structure of "+course_id)
                structure = courseid_to_structure[course_id]
                blocks_dict = {}
                block_queue = queue.Queue()
                # construct a dictory which contains all blocks and get the
                # root course block
                blocks = structure.get("blocks")
                for block in blocks:
                    blocks_dict[block.get("block_id")] = block
                    if block.get("block_type") == "course":
                        block_queue.put(block)
                        courses[course_id] = block
                # fill in the children field
                while not block_queue.empty():
                    block = block_queue.get()
                    block.pop("edit_info", None)
                    fields = block.get("fields")
                    if fields is None:
                        continue
                    block_type = block.get("block_type")
                    prefix = block.get("prefix") or ""
                    parent = block.get("parent") or block
                    children = fields.get("children")
                    if not children:
                        continue
                    # construct new_children
                    new_children = []
                    for c_idx, child in enumerate(children):
                        if child[0] not in target_block_type:
                            continue
                        child_one = blocks_dict.get(child[1])
                        child_one_fields = child_one.get('fields')
                        display_name = child_one_fields and child_one_fields.get('display_name')
                        display_name = display_name or ""
                        child_one["parent"] = parent
                        child_one["prefix"] = prefix + str(c_idx) + section_sep +\
                            str(display_name) + section_sep
                        new_children.append(child_one)
                        block_queue.put(child_one)
                        blocks.remove(child_one)
                    # assign new_children to parent
                    if block_type == "course":
                        parent["children"] = new_children
                    else:
                        parent["children"].remove(block)
                        parent["children"].extend(new_children)
            raw_data[RD_COURSE_IN_MONGO] = courses
        else:
            warn("COURSE_IN_MONGO can not be generated properly, the reasons may be:")
            warn("module_structure_filename is " + str(module_structure_filename))
            warn("length of structureid_to_courseid is " +
                 str(len(structureid_to_courseid)))
        raw_data[RD_DB] = MongoDB(DBc.DB_HOST, DBc.DB_NAME)
        return raw_data
