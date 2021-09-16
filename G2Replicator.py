#! /usr/bin/env python3

import os
import argparse
import sys
import io
import csv
import json
from datetime import datetime
import hashlib
import zlib

import G2Paths
from G2Database import G2Database
from G2Product import G2Product
from G2Engine import G2Engine
from G2IniParams import G2IniParams
from G2ConfigMgr import G2ConfigMgr
from G2Exception import G2Exception

class Replicator():

    #---------------------------------------
    def __init__(self, iniFileName, g2Engine, datamartConnectionStr, **kwargs):

        self.debug_level = kwargs['debug_level'] if 'debug_level' in kwargs else 0
        self.calculate_reports = kwargs['calculate_reports'] if 'calculate_reports' in kwargs else True

        self.stat_log = {}
        self.max_resume_hash_len = 250

        self.custom_entity_fields = False
        self.custom_record_fields = False
        self.custom_relation_fields = False
        self.custom_alert_processor = False

        try: 
            g2iniParams = G2IniParams()
            iniParams = g2iniParams.getJsonINIParams(iniFileName)
        except Exception as err:
            raise Exception(err) 

        #--use the process's engine if supplied
        if g2Engine:
            self.g2Engine = g2Engine
        else: 
            try: 
                self.g2Engine = G2Engine()
                self.g2Engine.initV2('G2Replicator', iniParams, False)
            except Exception as err:
                raise Exception(err) 

        #--each thread needs its own database connection
        try: self.dbo = G2Database(datamartConnectionStr)
        except Exception as err:
            raise Exception(err)

        self.get_entity_flags = 0
        self.get_entity_flags = self.get_entity_flags | self.g2Engine.G2_ENTITY_INCLUDE_ENTITY_NAME
        self.get_entity_flags = self.get_entity_flags | self.g2Engine.G2_ENTITY_INCLUDE_RECORD_DATA
        self.get_entity_flags = self.get_entity_flags | self.g2Engine.G2_ENTITY_INCLUDE_ALL_RELATIONS
        self.get_entity_flags = self.get_entity_flags | self.g2Engine.G2_ENTITY_INCLUDE_RELATED_MATCHING_INFO
        self.get_entity_flags = self.get_entity_flags | self.g2Engine.G2_ENTITY_INCLUDE_RELATED_RECORD_SUMMARY
        #self.get_entity_flags = self.get_entity_flags | self.g2Engine.G2_ENTITY_INCLUDE_RECORD_JSON_DATA
        #self.get_entity_flags = self.get_entity_flags | self.g2Engine.G2_ENTITY_INCLUDE_RECORD_MATCHING_INFO
        #self.get_entity_flags = self.get_entity_flags | self.g2Engine.G2_ENTITY_INCLUDE_RECORD_FORMATTED_DATA
        #self.get_entity_flags = self.get_entity_flags | self.g2Engine.G2_ENTITY_INCLUDE_RELATED_ENTITY_NAME

        self.get_record_flags = 0
        self.get_record_flags = self.get_record_flags | self.g2Engine.G2_ENTITY_INCLUDE_RECORD_JSON_DATA

        #--match category descriptions
        self.related_category_desc = {'DR': 'DISCLOSED_RELATION',
                                      'AM': 'AMBIGUOUS_MATCH',
                                      'PM': 'POSSIBLE_MATCH',
                                      'PR': 'POSSIBLY_RELATED'}

    #---------------------------------------
    def replicate(self, response_str):
        self.replication_status = 0
        self.replication_dt = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')

        response_json = json.loads(response_str)
        # {
        #   "DATA_SOURCE": "CUSTOMER",
        #   "RECORD_ID": "1001",
        #   "AFFECTED_ENTITIES": [
        #     {
        #       "ENTITY_ID": 1,
        #       "LENS_CODE": "DEFAULT"
        #     }
        #   ],
        #   "INTERESTING_ENTITIES": []
        # }

        in_data_source = response_json['DATA_SOURCE'] 
        in_record_id = response_json['RECORD_ID']

        self.debug_print()
        self.debug_print('-' * 50)
        self.debug_print('incoming', 'record', f'{in_data_source}: {in_record_id}')
        self.debug_print('withinfo-message', response_json)

        #--hopefully the only time a record needs to be added is here!
        #--however, the only time you really know what entity_id was assigned to the incoming record
        #--is if there is only one affected entity, otherwise set it to 0 and the correct
        #--affected entity will update it
        if len(response_json['AFFECTED_ENTITIES']) == 1: 
            entity_id = response_json['AFFECTED_ENTITIES'][0]['ENTITY_ID']
        else:
            entity_id = 0
        self.sync_dm_record(in_data_source, in_record_id, entity_id)
        self.debug_print('-' * 50)

        #--sync each affected entity
        full_resync_list = []
        entity_level = 0
        for affected_entity in response_json['AFFECTED_ENTITIES']:
            entity_id = affected_entity['ENTITY_ID']
            resync_entity_list = self.replicate_entity(entity_id, f'affected entity {entity_level}')
            full_resync_list.extend(resync_entity_list)
            entity_level += 1

        #--must also sync any newly related entities
        new_resync_list = []
        for related_id in set(full_resync_list):
            resync_entity_list = self.replicate_entity(related_id, 'related cycle 1')
            new_resync_list.extend(resync_entity_list)

        #--capture any leftover related entities for possible recursive process
        if new_resync_list: 
            self.log_stat('replicate', 'leftover entities', ', '.join([str(x) for x in new_resync_list]))

        if self.custom_alert_processor:
            for interesting_entity_data in response_json['INTERESTING_ENTITIES']:
                self.process_interesting_entity(in_data_source, in_record_id, interesting_entity_data)

        return self.replication_status

    #---------------------------------------
    def replicate_entity(self, entity_id, sync_type):

        #--setting this again as process may  be called directly to resync an entire entity
        called_by = sys._getframe().f_back.f_code.co_name
        if called_by != 'replicate':
            self.replication_dt = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')

        current_entity_reference = f'entity_id: {entity_id}'
        if self.debug_level:
            print()
        self.log_stat('request', sync_type, current_entity_reference)

        #--get current entity summary
        g2_entity_resume = self.get_resume_g2_api(entity_id)
        self.debug_print('g2_resume', g2_entity_resume)

        #--get prior entity summary and bypass if no changes
        dm_entity_resume = self.get_resume_dm(entity_id)
        if dm_entity_resume['RESUME_HASH'] == g2_entity_resume['RESUME_HASH']:
            self.log_stat(sync_type, 'no_change', entity_id)
            return [] #--is expecting a list of related entities to sync
        elif dm_entity_resume['RESUME_HASH']:
            self.debug_print('g2_resume_hash', g2_entity_resume['RESUME_HASH'])
            self.debug_print('dm_resume_hash', dm_entity_resume['RESUME_HASH'])

        #--expand the prior entity resume or rebuild it from the datamart
        dm_entity_resume = self.expand_resume_dm(dm_entity_resume)
        self.debug_print('dm_resume', dm_entity_resume)

        #--note the resume contains entity size which should be zero if deleted
        self.sync_dm_entity(entity_id, sync_type, g2_entity_resume)

        #--perform a net change to see what records and relationships to add/delete
        nc_entity_resume = self.net_change_resume(entity_id, g2_entity_resume, dm_entity_resume)
        self.debug_print('nc_resume', nc_entity_resume)

        #--calculate and update reports: entity size breakdown, data and cross source summaries
        if self.calculate_reports:
            self.net_change_report(entity_id, nc_entity_resume)

        #--de-dupe list of related entities to resync
        resync_entity_list = nc_entity_resume['RESYNC_ENTITY_LIST']

        return resync_entity_list

    #---------------------------------------
    def net_change_resume(self, entity_id, g2_entity_resume, dm_entity_resume):

        rebuild_all_records = False #--might add this paramter for a resync later

        #--all the relationships should be rebuilt if this entity gains or loses a data source
        data_source_list_changed = False

        #--this process prepares for report generation by creating a report summary that includes
        #--both records and relationships.  
        #--doing it here means we only have to roll through the list of relationships once.
        nc_entity_resume = {}
        nc_entity_resume['G2_REPORT_SUMMARY'] = {'RESOLVED': g2_entity_resume['RECORD_SUMMARY']}
        nc_entity_resume['DM_REPORT_SUMMARY'] = {'RESOLVED': dm_entity_resume['RECORD_SUMMARY']}

        #--new records to add
        for data_source in g2_entity_resume['RECORD_SUMMARY']:
            for record_id in g2_entity_resume['RECORD_SUMMARY'][data_source]:
                current_record_reference = f"{data_source}: {record_id}"
                record = {'DATA_SOURCE': data_source, 'RECORD_ID': record_id}
                if not (data_source in dm_entity_resume['RECORD_SUMMARY'] and \
                        record_id in dm_entity_resume['RECORD_SUMMARY'][data_source]):

                    if data_source not in dm_entity_resume['RECORD_SUMMARY']:
                        data_source_list_changed = True

                    if rebuild_all_records:
                        self.sync_dm_record(record['DATA_SOURCE'], record['RECORD_ID'], entity_id)
                    else:
                        db_response = self.attach_dm_record(current_record_reference, record['DATA_SOURCE'], record['RECORD_ID'], entity_id)
                        if db_response == 0:
                            self.log_stat('record', 'attach_succeeded', current_record_reference)
                        else:
                            #--the record to attach is missing, so we must do a full update of the record
                            #--this can easily happen when two records got added to an entity before being replicated
                            #--the hash will be the same the 2nd time through and the second record never added!
                            self.log_stat('record', 'missing', current_record_reference)
                            self.sync_dm_record(record['DATA_SOURCE'], record['RECORD_ID'], entity_id)

        #--old records to delete
        for data_source in dm_entity_resume['RECORD_SUMMARY']:
            deleted_cnt = 0
            for record_id in dm_entity_resume['RECORD_SUMMARY'][data_source]:
                current_record_reference = f"{data_source}: {record_id}"
                record = {'DATA_SOURCE': data_source, 'RECORD_ID': record_id}
                if not (data_source in g2_entity_resume['RECORD_SUMMARY'] and \
                        record_id in g2_entity_resume['RECORD_SUMMARY'][data_source]):
                    deleted_cnt += 1
                    db_response = self.detach_dm_record(current_record_reference, record['DATA_SOURCE'], record['RECORD_ID'], entity_id)
                    if db_response == 0:
                        self.log_stat('record', 'detached_to_nowhere', current_record_reference)
                    else:
                        self.log_stat('record', 'detach_unnecessary', current_record_reference)

            #--if all records for this data source were removed  
            if deleted_cnt == len(dm_entity_resume['RECORD_SUMMARY'][data_source]):
                data_source_list_changed = True


        #--prior entities to resynch
        nc_entity_resume['RESYNC_ENTITY_LIST'] = [] 

        #--new relationships to add or update
        for related_id in g2_entity_resume['RELATION_SUMMARY']:
            db_action = None
            if related_id not in dm_entity_resume['RELATION_SUMMARY']:
                db_action = 'add'
            else:
                if dm_entity_resume['RELATION_SUMMARY'][related_id] != \
                   g2_entity_resume['RELATION_SUMMARY'][related_id]:
                    db_action = 'Update'
            if db_action:
                match_level = g2_entity_resume['RELATION_SUMMARY'][related_id]['MATCH_LEVEL']
                match_category = g2_entity_resume['RELATION_SUMMARY'][related_id]['MATCH_CATEGORY']
                match_key = g2_entity_resume['RELATION_SUMMARY'][related_id]['MATCH_KEY']
                data_sources = self.make_csv_string(g2_entity_resume['RELATION_SUMMARY'][related_id]['DATA_SOURCES'])
                response = self.upsert_dm_relation(entity_id,
                                                   related_id,
                                                   match_level,
                                                   match_category,
                                                   match_key,
                                                   data_sources)
                if response == 0: #--success
                    self.log_stat('relation', db_action, f'entity_id: {entity_id}, related_id: {related_id}')

                    #--log related entity to capture relationship from their point of view
                    nc_entity_resume['RESYNC_ENTITY_LIST'].append(int(related_id))

            #--also trigger related entity if list of data sources changed
            elif data_source_list_changed:
                nc_entity_resume['RESYNC_ENTITY_LIST'].append(int(related_id))

            #--add relationship for report summary
            match_category = g2_entity_resume['RELATION_SUMMARY'][related_id]['MATCH_CATEGORY']
            if match_category not in nc_entity_resume['G2_REPORT_SUMMARY']:
                nc_entity_resume['G2_REPORT_SUMMARY'][match_category] = {}
            for data_source in g2_entity_resume['RELATION_SUMMARY'][related_id]['DATA_SOURCES']:
                if data_source not in nc_entity_resume['G2_REPORT_SUMMARY'][match_category]:
                    nc_entity_resume['G2_REPORT_SUMMARY'][match_category][data_source] = [related_id]
                else:
                    nc_entity_resume['G2_REPORT_SUMMARY'][match_category][data_source].append(related_id)

        #--old relationships to delete
        for related_id in dm_entity_resume['RELATION_SUMMARY']:
            if related_id not in g2_entity_resume['RELATION_SUMMARY']:
                response = self.delete_dm_relation(entity_id, int(related_id))
                if response == 0: #--success
                    self.log_stat('relation', 'delete', f'entity_id: {entity_id}, related_id: {related_id}')

                    #--log related entity to capture relationship from their point of view
                    nc_entity_resume['RESYNC_ENTITY_LIST'].append(int(related_id))

            #--add relationship to report summary
            match_category = dm_entity_resume['RELATION_SUMMARY'][related_id]['MATCH_CATEGORY']
            if match_category not in nc_entity_resume['DM_REPORT_SUMMARY']:
                nc_entity_resume['DM_REPORT_SUMMARY'][match_category] = {}
            for data_source in dm_entity_resume['RELATION_SUMMARY'][related_id]['DATA_SOURCES']:
                if data_source not in nc_entity_resume['DM_REPORT_SUMMARY'][match_category]:
                    nc_entity_resume['DM_REPORT_SUMMARY'][match_category][data_source] = [related_id]
                else:
                    nc_entity_resume['DM_REPORT_SUMMARY'][match_category][data_source].append(related_id)

        return nc_entity_resume

    #---------------------------------------
    #--report processing
    #---------------------------------------

    #---------------------------------------
    def net_change_report(self, entity_id, nc_entity_resume):

        g2_report_stats = self.calc_report_stats(entity_id, nc_entity_resume['G2_REPORT_SUMMARY'])
        self.debug_print('g2_report_stats', g2_report_stats)

        dm_report_stats = self.calc_report_stats(entity_id, nc_entity_resume['DM_REPORT_SUMMARY'])
        self.debug_print('dm_report_stats', dm_report_stats)

        #--perform net change update of current g2 report stats
        for report_key in g2_report_stats:
            report_data = g2_report_stats[report_key]
            if report_key in dm_report_stats:
                if dm_report_stats[report_key] == g2_report_stats[report_key]:
                    self.log_stat('report_key', 'same', report_key)
                    continue
                
                report_data['ENTITY_COUNT'] = \
                    self.get_stat_count(g2_report_stats[report_key], 'ENTITY_COUNT') - \
                    self.get_stat_count(dm_report_stats[report_key], 'ENTITY_COUNT')
                report_data['RECORD_COUNT'] = \
                    self.get_stat_count(g2_report_stats[report_key], 'RECORD_COUNT') - \
                    self.get_stat_count(dm_report_stats[report_key], 'RECORD_COUNT')
                report_data['RELATION_COUNT'] = \
                    self.get_stat_count(g2_report_stats[report_key], 'RELATION_COUNT') - \
                    self.get_stat_count(dm_report_stats[report_key], 'RELATION_COUNT')

                #--note: the entity_id for resolved statistics does not need to be re-added here!
                #--but the list of related_ids might have changed!
                report_data['ADD_RELATED_IDS'] = []
                report_data['DELETE_RELATED_IDS'] = []
                if 'RELATED_IDS' in g2_report_stats[report_key]:
                    for related_id in g2_report_stats[report_key]['RELATED_IDS']:
                        if str(related_id) not in dm_report_stats[report_key]['RELATED_IDS']:
                            report_data['ADD_RELATED_IDS'].append(related_id)
                    for related_id in dm_report_stats[report_key]['RELATED_IDS']:
                        if str(related_id) not in g2_report_stats[report_key]['RELATED_IDS']:
                            report_data['DELETE_RELATED_IDS'].append(related_id)
                self.log_stat('report_key', 'updated', report_key)
                self.debug_print('report_key', report_key, report_data)

            else:
                self.debug_print('report_key', report_key, 'new')

                if 'RELATED_IDS' in g2_report_stats[report_key]:
                    report_data['ADD_RELATED_IDS'] = g2_report_stats[report_key]['RELATED_IDS']
                elif 'ENTITY_ID' in g2_report_stats[report_key]:
                    report_data['ADD_ENTITY_ID'] = g2_report_stats[report_key]['ENTITY_ID']

            response = self.sync_dm_report(report_data)
            if response != 0:
                self.debug_print('g2 stat record', g2_report_stats[report_key])
                self.debug_print('dm stat record', dm_report_stats[report_key] if report_key in dm_report_stats else 'not found')
                self.debug_print('diff record', report_data)
                input('press any key ..')

        #--undo prior stats that are no longer valid
        for report_key in dm_report_stats:
            if report_key not in g2_report_stats:
                self.log_stat('report_key', 'deleted', report_key)
                report_data = dm_report_stats[report_key]
                report_data['ENTITY_COUNT'] = -self.get_stat_count(dm_report_stats[report_key], 'ENTITY_COUNT')
                report_data['RECORD_COUNT'] = -self.get_stat_count(dm_report_stats[report_key], 'RECORD_COUNT')
                report_data['RELATION_COUNT'] = -self.get_stat_count(dm_report_stats[report_key], 'RELATION_COUNT')
                if 'RELATED_IDS' in dm_report_stats[report_key]:
                    report_data['DELETE_RELATED_IDS'] = dm_report_stats[report_key]['RELATED_IDS']
                elif 'ENTITY_ID' in dm_report_stats[report_key]:
                    report_data['DELETE_ENTITY_ID'] = dm_report_stats[report_key]['ENTITY_ID']

                response = self.sync_dm_report(report_data)
                if response != 0:
                    self.debug_print('diff record', report_data)
                    input('press any key ..')


    #---------------------------------------
    def get_stat_count(self, _dict, _key):
        if _key in _dict:
            return _dict[_key] 
        return 0

    #---------------------------------------
    def calc_report_stats(self, entity_id, report_summary):
        report_stats = {}

        #--stats are captured from the resolved entity's point of view
        #-- so match_level1 can only be resolved
        match_level1 = 'RESOLVED'
        total_record_count = 0
        for data_source1 in report_summary[match_level1]:
            record_count = len(report_summary[match_level1][data_source1])
            total_record_count += record_count

            report_data = {'REPORT': 'DSS', 
                           'STATISTIC': 'ENTITY_COUNT',
                           'DATA_SOURCE1': data_source1,
                           'DATA_SOURCE2': data_source1,
                           'ENTITY_COUNT': 1}
            report_stats[self.calc_report_key(report_data)] = report_data

            report_data = {'REPORT': 'DSS', 
                           'STATISTIC': 'SINGLE_COUNT' if record_count == 1 else 'DUPLICATE_COUNT',
                           'DATA_SOURCE1': data_source1,
                           'DATA_SOURCE2': data_source1,
                           'ENTITY_COUNT': 1,
                           'RECORD_COUNT': record_count, 
                           'ENTITY_ID': entity_id if record_count > 1 else None}
            report_stats[self.calc_report_key(report_data)] = report_data

            #--now check what other resolutions and relationships the "data source" entity above has
            for match_level2 in report_summary:
                for data_source2 in report_summary[match_level2]:
                    if match_level2 == match_level1 and data_source2 == data_source1:
                        continue  #--skip the level1 "data source" entity above
 
                    if match_level2 == 'RESOLVED':
                        report_data = {'REPORT': 'CSS', #--has to be cross source if level 2 is still resolved 
                                       'STATISTIC': 'MATCHED_COUNT',
                                       'DATA_SOURCE1': data_source1,
                                       'DATA_SOURCE2': data_source2,
                                       'ENTITY_COUNT': 1,
                                       'RECORD_COUNT': record_count, #--from data_source1's point of view
                                       'ENTITY_ID': entity_id}
                        report_stats[self.calc_report_key(report_data)] = report_data
 
                    else: # its a relationship in either the same source or a different one:
                        related_id_list = report_summary[match_level2][data_source2]
                        report_data = {'REPORT': 'DSS' if data_source2 == data_source1 else 'CSS', 
                                       'STATISTIC': self.related_category_desc[match_level2] + '_COUNT',
                                       'DATA_SOURCE1': data_source1,
                                       'DATA_SOURCE2': data_source2,
                                       'ENTITY_COUNT': 1,
                                       'RELATION_COUNT': len(related_id_list),
                                       'ENTITY_ID': entity_id,
                                       'RELATED_IDS': related_id_list}
                        report_stats[self.calc_report_key(report_data)] = report_data

        #--entity size breakdown
        if total_record_count > 0:
            report_data = {'REPORT': 'ESB', 
                           'STATISTIC': str(total_record_count),
                           'DATA_SOURCE1': 'n/a',
                           'DATA_SOURCE2': 'n/a',
                           'ENTITY_COUNT': 1,
                           'ENTITY_ID': entity_id}
            report_stats[self.calc_report_key(report_data)] = report_data

        return report_stats

    #---------------------------------------
    def calc_report_key(self, report_data):
        return report_data['REPORT'] + '|' + report_data['STATISTIC'] + \
               ('|' + report_data['DATA_SOURCE1'] if 'DATA_SOURCE1' in report_data else '') + \
               ('|' + report_data['DATA_SOURCE2'] if 'DATA_SOURCE2' in report_data else '')

    #---------------------------------------
    #--dm_entity database calls
    #---------------------------------------

    #---------------------------------------
    def sync_dm_entity(self, entity_id, sync_type, g2_entity_resume):
        current_entity_reference = f'entity_id: {entity_id}'

        #--entity must have been deleted
        if g2_entity_resume['RECORD_COUNT'] == 0:
            response = self.delete_dm_entity(entity_id)
            if response == 0: #--success
                self.log_stat(sync_type, 'delete', current_entity_reference)
                self.sync_dm_report({'REPORT': 'TOTAL', 'STATISTIC': 'ENTITY_COUNT', 'ENTITY_COUNT': -1})
            return

        insert_fields = ['ENTITY_ID', 
                         'ENTITY_NAME', 
                         'RECORD_COUNT', 
                         'RELATION_COUNT', 
                         'RESUME_HASH',
                         'FIRST_SEEN_DT', 
                         'LAST_SEEN_DT']
        insert_values = [entity_id, 
                         g2_entity_resume['ENTITY_NAME'], 
                         g2_entity_resume['RECORD_COUNT'], 
                         g2_entity_resume['RELATION_COUNT'], 
                         g2_entity_resume['RESUME_HASH'], 
                         self.replication_dt, 
                         self.replication_dt]
        update_fields = ['ENTITY_NAME', 
                         'RECORD_COUNT', 
                         'RELATION_COUNT', 
                         'RESUME_HASH',
                         'LAST_SEEN_DT']
        update_values = [g2_entity_resume['ENTITY_NAME'], 
                         g2_entity_resume['RECORD_COUNT'], 
                         g2_entity_resume['RELATION_COUNT'], 
                         g2_entity_resume['RESUME_HASH'], 
                         self.replication_dt]

        #--add any custom fields from the actual json record
        if self.custom_entity_fields:
            custom_fields, custom_values = self.custom_dm_entity_fields(g2_entity_resume)
            if custom_fields:
                insert_fields.extend(custom_fields)
                insert_values.extend(custom_values)
                update_fields.extend(custom_fields)
                update_values.extend(custom_values)

        insert_success = False
        update_success = False

        #--try insert first on level 0 
        #--this may or may not be worth it, but entity should already exist on all other sync types
        if sync_type == 'affected entity 0':
            response = self.insert_dm_entity(entity_id, insert_fields, insert_values)
            insert_success = True if response == 0 else False
            if response == 1: #--duplicate key 
                update_values.append(entity_id)
                response = self.update_dm_entity(entity_id, update_fields, update_values)
                update_success = True if response == 0 else False

        #--try update first as prior entity was changed
        else:
            update_values.append(entity_id)
            response = self.update_dm_entity(entity_id, update_fields, update_values)
            update_success = True if response == 0 else False
            if response == 1: #--no rows updated 
                response = self.insert_dm_entity(entity_id, insert_fields, insert_values)
                update_success = True if response == 0 else False

        if insert_success:
            self.log_stat(sync_type, 'insert', current_entity_reference)
            self.sync_dm_report({'REPORT': 'TOTAL', 'STATISTIC': 'ENTITY_COUNT', 'ENTITY_COUNT': 1})

        elif update_success:
            self.log_stat(sync_type, 'update', current_entity_reference)

    #---------------------------------------
    def insert_dm_entity(self, entity_id, insert_fields, insert_values):
        sql_stmt = 'insert into DM_ENTITY (' + ','.join(insert_fields) + ')'
        sql_stmt += ' values (' + ','.join(['?'] * len(insert_fields)) + ')'
        try: self.dbo.sqlExec(sql_stmt, insert_values)
        except Exception as err:
            if 'UNIQUE' in str(err).upper():
                return 1 #--duplicate key violation
            else:
                self.log_stat('sql_error', 'insert_entity', f'entity_id: {entity_id}')
                self.debug_print('\t' + str(err))
                self.replication_status = 2 #--sql error
                return 2
        return 0 #--success

    #---------------------------------------
    def update_dm_entity(self, entity_id, update_fields, update_values):
        sql_stmt = 'update DM_ENTITY set ' + ','.join(['%s = ?' % x for x in update_fields])
        sql_stmt += ' where ENTITY_ID = ?'
        try: db_response = self.dbo.sqlExec(sql_stmt, update_values)
        except Exception as err:
            self.log_stat('sql_error', 'update_entity', f'entity_id: {entity_id}')
            self.debug_print('sql_error', str(err))
            self.replication_status = 2 #--sql error
            return 2
        return 0 if db_response['ROWS_AFFECTED'] == 1 else 1

    #---------------------------------------
    def delete_dm_entity(self, entity_id):
        sql_stmt = 'delete from DM_ENTITY where ENTITY_ID = ?'
        try: db_response = self.dbo.sqlExec(sql_stmt, entity_id)
        except Exception as err:
            self.log_stat('sql_error', 'delete_entity', f'entity_id: {entity_id}')
            self.debug_print('sql_error', str(err))
            self.replication_status = 2 #--sql error
            return 2
        return 0 if db_response['ROWS_AFFECTED'] == 1 else 1

    #---------------------------------------
    #--dm_record database calls
    #---------------------------------------

    #---------------------------------------
    def sync_dm_record(self, data_source, record_id, entity_id):
        current_record_reference = f'{data_source}: {record_id}'

        if entity_id < 0: #-- the record must have been deleted
            response = self.delete_dm_record(current_record_reference, data_source, record_id)
            if response == 0: #--success
                self.log_stat('record', 'delete', current_record_reference)
                self.sync_dm_report({'REPORT': 'DSS', 'DATA_SOURCE1': data_source, 'RECORD_COUNT': -1})
            return

        insert_fields = ['DATA_SOURCE', 'RECORD_ID', 'ENTITY_ID', 'FIRST_SEEN_DT', 'LAST_SEEN_DT']
        insert_values = [data_source, record_id, entity_id, self.replication_dt, self.replication_dt]
        update_fields = ['ENTITY_ID', 'LAST_SEEN_DT']
        update_values = [entity_id, self.replication_dt]

        #--add any custom fields from the json data
        if self.custom_record_fields:
            try: 
                response = bytearray()
                retcode = self.g2Engine.getRecordV2(data_source, record_id, self.get_record_flags, response)
                response = response.decode() if response else ''
            except G2Exception as err:
                self.log_stat('api_error', 'getRecordV2', stat_log_reference)
                self.replication_status = 1 #--api error
                return 
            record_data = json.loads(response)
            custom_fields, custom_values = self.custom_dm_record_fields(data_source, record_id, record_data['JSON_DATA'])
            if custom_fields:
                insert_fields.extend(custom_fields)
                insert_values.extend(custom_values)
                update_fields.extend(custom_fields)
                update_values.extend(custom_values)

        #--always try insert first on records
        response = self.insert_dm_record(current_record_reference, insert_fields, insert_values)
        if response == 0: #--success
            self.log_stat('record', 'insert', current_record_reference)
            self.sync_dm_report({'REPORT': 'DSS', 
                                 'DATA_SOURCE1': data_source, 
                                 'STATISTIC': 'RECORD_COUNT', 
                                 'RECORD_COUNT': 1})

        elif response == 1: #--duplicate key
            update_values.append(data_source)
            update_values.append(record_id)
            response = self.update_dm_record(current_record_reference, update_fields, update_values)
            if response == 0:
                self.log_stat('record', 'update', current_record_reference)

    #---------------------------------------
    def insert_dm_record(self, current_record_reference, insert_fields, insert_values):
        sql_stmt = 'insert into DM_RECORD (' + ','.join(insert_fields) + ')'
        sql_stmt += ' values (' + ','.join(['?'] * len(insert_fields)) + ')'
        try: self.dbo.sqlExec(sql_stmt, insert_values)
        except Exception as err:
            if 'UNIQUE' in str(err).upper():
                return 1 #--duplicate key violation
            else:
                self.log_stat('sql_error', 'insert_record', current_record_reference)
                self.debug_print('sql_error', str(err))
                self.replication_status = 2 #--sql error
                return 2
        return 0 #--success

    #---------------------------------------
    def update_dm_record(self, current_record_reference, update_fields, update_values):
        sql_stmt = 'update DM_RECORD set ' + ','.join(['%s = ?' % x for x in update_fields])
        sql_stmt += ' where DATA_SOURCE = ? and RECORD_ID = ?'
        try: db_response = self.dbo.sqlExec(sql_stmt, update_values)
        except Exception as err:
            self.log_stat('sql_error', 'update_record', current_record_reference)
            self.debug_print('sql_error', str(err))
            self.replication_status = 2 #--sql error
            return 2
        return 0 if db_response['ROWS_AFFECTED'] == 1 else 1

    #---------------------------------------
    def delete_dm_record(self, current_record_reference, data_source, record_id):
        sql_stmt = 'delete from DM_RECORD where DATA_SOURCE = ? and RECORD_ID = ?'
        try: db_response = self.dbo.sqlExec(sql_stmt, [data_source, record_id])
        except Exception as err:
            self.log_stat('sql_error', 'delete_record', current_record_reference)
            self.debug_print('sql_error', str(err))
            self.replication_status = 2 #--sql error
            return 2
        return 0 if db_response['ROWS_AFFECTED'] == 1 else 1

    #---------------------------------------
    def attach_dm_record(self, current_record_reference, data_source, record_id, entity_id):
        sql_stmt = 'update DM_RECORD set ENTITY_ID = ? where DATA_SOURCE = ? and RECORD_ID = ?'
        try: db_response = self.dbo.sqlExec(sql_stmt, [entity_id, data_source, record_id])
        except Exception as err:
            self.log_stat('sql_error', 'attach_record', current_record_reference)
            self.debug_print('sql_error', str(err))
            #self.debug_print('sql_error', sql_stmt)
            #self.debug_print('sql_error', ','.join([str(x) for x in [entity_id, data_source, record_id]]))
            self.replication_status = 2 #--sql error
            return 2
        return 0 if db_response['ROWS_AFFECTED'] == 1 else 1

    #---------------------------------------
    def detach_dm_record(self, current_record_reference, data_source, record_id, entity_id):
        #--only move to nowhere if still attached to the current entity
        #--it either has or is going to move
        sql_stmt = 'update DM_RECORD set ENTITY_ID = -1 ' \
                   'where DATA_SOURCE = ? and RECORD_ID = ? and ENTITY_ID = ?'
        try: db_response = self.dbo.sqlExec(sql_stmt, [data_source, record_id, entity_id])
        except Exception as err:
            self.log_stat('sql_error', 'detach_record', current_record_reference)
            self.debug_print('sql_error', str(err))
            #self.debug_print('sql_error', sql_stmt)
            #self.debug_print('sql_error', ','.join([str(x) for x in [entity_id, data_source, record_id]]))
            self.replication_status = 2 #--sql error
            return 2
        return 0 if db_response['ROWS_AFFECTED'] == 1 else 1

    #---------------------------------------
    #--dm_relationsip database calls
    #---------------------------------------

    #---------------------------------------
    def upsert_dm_relation(self, entity_id, related_id, match_level, match_category, match_key, data_sources):
        insert_fields = ['ENTITY_ID', 
                         'RELATED_ID', 
                         'MATCH_LEVEL', 
                         'MATCH_KEY', 
                         'MATCH_CATEGORY',
                         'DATA_SOURCES', 
                         'FIRST_SEEN_DT', 
                         'LAST_SEEN_DT']
        insert_values = [entity_id, 
                         int(related_id),  #--its a string as it came in from dictionary key 
                         match_level, 
                         match_key, 
                         match_category, 
                         data_sources,
                         self.replication_dt, 
                         self.replication_dt]
        update_fields = ['MATCH_LEVEL', 
                         'MATCH_KEY', 
                         'MATCH_CATEGORY', 
                         'DATA_SOURCES', 
                         'LAST_SEEN_DT']
        update_values = [match_level, 
                         match_key, 
                         match_category, 
                         data_sources,
                         self.replication_dt]
        sql_stmt = 'insert into DM_RELATION (' + ','.join(insert_fields) + ')' + \
                   ' values (' + ','.join(['?'] * len(insert_fields)) + ')' + \
                   ' on conflict (ENTITY_ID, RELATED_ID) do update set '  + \
                   ','.join(['%s = ?' % x for x in update_fields])
        try: db_response = self.dbo.sqlExec(sql_stmt, insert_values + update_values)
        except Exception as err:
            self.log_stat('sql_error', 'upsert_relation', f'entity_id: {entity_id}, related_id: {related_id}')
            self.debug_print('sql_error', str(err))
            self.replication_status = 2 #--sql error
            return 2
        return 0 if db_response['ROWS_AFFECTED'] == 1 else 1

    #---------------------------------------
    def delete_dm_relation(self, entity_id, related_id):
        sql_stmt = 'delete from DM_RELATION where ENTITY_ID = ? and RELATED_ID = ?'
        try: db_response = self.dbo.sqlExec(sql_stmt, [entity_id, related_id])
        except Exception as err:
            self.log_stat('sql_error', 'delete_relation', f'entity_id: {entity_id}, related_id: {related_id}')
            self.debug_print('sql_error', str(err))
            self.replication_status = 2 #--sql error
            return 2
        return 0 if db_response['ROWS_AFFECTED'] == 1 else 1

    #---------------------------------------
    #--dm_relationsip database calls
    #---------------------------------------

    #---------------------------------------
    def sync_dm_report(self, report_data):
        report_key = self.calc_report_key(report_data)
        entity_count = report_data['ENTITY_COUNT'] if 'ENTITY_COUNT' in report_data else 0
        record_count = report_data['RECORD_COUNT'] if 'RECORD_COUNT' in report_data else 0
        relation_count = report_data['RELATION_COUNT'] if 'RELATION_COUNT' in report_data else 0

        #--try update first as the actual statistic only needs to be added once
        dm_report_action = 'update'
        response = self.update_dm_report(report_key, entity_count, record_count, relation_count)
        if response != 0:
            dm_report_action = 'insert'
            response = self.insert_dm_report(report_key, entity_count, record_count, relation_count, report_data)

        detail_updated = False
        if 'ADD_ENTITY_ID' in report_data and report_data['ADD_ENTITY_ID'] and response == 0:
            detail_updated = True
            response = self.insert_dm_report_detail(report_key, report_data['ADD_ENTITY_ID'])

        if 'DELETE_ENTITY_ID' in report_data and report_data['DELETE_ENTITY_ID'] and response == 0:
            detail_updated = True
            response = self.delete_dm_report_detail(report_key, report_data['DELETE_ENTITY_ID'])

        if 'ADD_RELATED_IDS' in report_data and report_data['ADD_RELATED_IDS'] and response == 0:
            detail_updated = True
            for related_id in report_data['ADD_RELATED_IDS']:
                response = self.insert_dm_report_detail(report_key, report_data['ENTITY_ID'], related_id)
                if response != 0:
                    break

        if 'DELETE_RELATED_IDS' in report_data  and report_data['DELETE_RELATED_IDS'] and response == 0:
            detail_updated = True
            for related_id in report_data['DELETE_RELATED_IDS']:
                response = self.delete_dm_report_detail(report_key, report_data['ENTITY_ID'], related_id)
                if response != 0:
                    break

        #--log stat update with no detail
        if not detail_updated:
            self.log_stat('report', dm_report_action, report_key)

        return response

    #---------------------------------------
    def update_dm_report(self, report_key, entity_count, record_count, relation_count):
        sql_stmt = 'update DM_REPORT set ' \
                   ' ENTITY_COUNT = ENTITY_COUNT + ?, ' \
                   ' RECORD_COUNT = RECORD_COUNT + ?, ' \
                   ' RELATION_COUNT = RELATION_COUNT + ? ' \
                   'where REPORT_KEY = ?'
        try: db_response = self.dbo.sqlExec(sql_stmt, (entity_count, record_count, relation_count, report_key))
        except: 
            self.log_stat('sql_error', 'update_dm_report', report_key)
            self.debug_print('sql_error', str(err))
            return 2
        return 0 if db_response['ROWS_AFFECTED'] == 1 else 1

    #---------------------------------------
    def insert_dm_report(self, report_key, entity_count, record_count, relation_count, report_data):
        sql_stmt = 'insert into DM_REPORT (' \
                   ' REPORT_KEY, ' \
                   ' REPORT, ' \
                   ' STATISTIC, ' \
                   ' DATA_SOURCE1, ' \
                   ' DATA_SOURCE2, ' \
                   ' ENTITY_COUNT, ' \
                   ' RECORD_COUNT, ' \
                   ' RELATION_COUNT) ' \
                   'values (?, ?, ?, ?, ?, ?, ?, ?)'
        insert_values = [report_key, 
                         report_data['REPORT'], 
                         report_data['STATISTIC'], 
                         report_data['DATA_SOURCE1'] if 'DATA_SOURCE1' in report_data else None, 
                         report_data['DATA_SOURCE2'] if 'DATA_SOURCE2' in report_data else None, 
                         entity_count, 
                         record_count, 
                         relation_count]
        try: db_response = self.dbo.sqlExec(sql_stmt, insert_values)
        except: 
            self.log_stat('sql_error', 'insert_dm_report', report_key)
            self.debug_print('sql_error', str(err))
            return 2
        return 0 if db_response['ROWS_AFFECTED'] == 1 else 1

    #---------------------------------------
    def insert_dm_report_detail(self, report_key, entity_id, related_id = 0):
        sql_stmt = 'insert into DM_REPORT_DETAIL (REPORT_KEY, ENTITY_ID, RELATED_ID) values (?, ?, ?)'
        sql_values = [report_key, entity_id, related_id]
        try: db_response = self.dbo.sqlExec(sql_stmt, sql_values)
        except Exception as err:
            #if 'UNIQUE' in str(err).upper():
            #    return 1 #--duplicate key violation
            #else:
            self.log_stat('sql_error', 'insert_dm_report_detail', ','.join([str(x) for x in sql_values]))
            self.debug_print('\t' + str(err))
            return 2
        else:
            self.log_stat('report_detail', 'insert', ','.join([str(x) for x in sql_values]))
        return 0 if db_response['ROWS_AFFECTED'] == 1 else 1

    #---------------------------------------
    def delete_dm_report_detail(self, report_key, entity_id, related_id = 0):
        sql_stmt = 'delete from DM_REPORT_DETAIL where REPORT_KEY = ? and ENTITY_ID = ? and RELATED_ID = ?'
        sql_values = [report_key, entity_id, related_id]
        try: db_response = self.dbo.sqlExec(sql_stmt, sql_values)
        except Exception as err:
            self.log_stat('sql_error', 'delete_dm_report_detail', ', '.join([str(x) for x in sql_values]))
            self.debug_print('sql_error', str(err))
            return 2
        return 0 if db_response['ROWS_AFFECTED'] == 1 else 1

    #---------------------------------------
    #--resume retrieval
    #---------------------------------------

    #---------------------------------------
    def get_resume_g2_api(self, entity_id):
        empty_resume = {'ENTITY_ID': entity_id, 
                        'ENTITY_NAME': 'not found!', 
                        'RECORD_COUNT': 0, 
                        'RELATION_COUNT': 0, 
                        'RECORD_SUMMARY': {}, 
                        'RELATION_SUMMARY': {},
                        'RESUME_HASH': ''}
        try: 
            response = bytearray()
            retcode = self.g2Engine.getEntityByEntityIDV2(int(entity_id), self.get_entity_flags, response)
            response = response.decode() if response else ''
        except G2Exception as err:
            print(str(err))
            #--note only return an empty entity summary if exception is entity not found
            #--as it may occur that a logged entity no longer exists
            #--otherwise this should be a fatal shutdown!
            return empty_resume
        if not response:
            print('warning: api response for entity %s is blank' % entity_id)
            return empty_resume

        json_data = json.loads(response)

        record_summary = {}
        for record in json_data['RESOLVED_ENTITY']['RECORDS']:
            if record['DATA_SOURCE'] not in record_summary:
                record_summary[record['DATA_SOURCE']] = []
            record_summary[record['DATA_SOURCE']].append(record['RECORD_ID'])

        relation_summary = {}
        for relation in json_data['RELATED_ENTITIES']:
            if relation['IS_DISCLOSED'] != 0:
                match_category = 'DR'
            elif relation['IS_AMBIGUOUS'] != 0:
                match_category = 'AM'
            elif relation['MATCH_LEVEL'] == 2:
                match_category = 'PM'
            else:          
                match_category = 'PR'
            dsrc_list = []
            for record in relation['RECORD_SUMMARY']:
                dsrc_list.append(record['DATA_SOURCE'])
            relation_data = {'MATCH_LEVEL': relation['MATCH_LEVEL'],
                             'MATCH_KEY': relation['MATCH_KEY'], 
                             'MATCH_CATEGORY': match_category,
                             'DATA_SOURCES': dsrc_list}
            relation_summary[str(relation['ENTITY_ID'])] = relation_data

        entity_resume = {}
        entity_resume['ENTITY_ID'] = json_data['RESOLVED_ENTITY']['ENTITY_ID']
        entity_resume['ENTITY_NAME'] = json_data['RESOLVED_ENTITY']['ENTITY_NAME']
        entity_resume['RECORD_COUNT'] = len(json_data['RESOLVED_ENTITY']['RECORDS'])
        entity_resume['RELATION_COUNT'] = len(json_data['RELATED_ENTITIES'])
        entity_resume['RECORD_SUMMARY'] = record_summary #--this is re-formated without json data and all that for report calculation
        entity_resume['RELATION_SUMMARY'] = relation_summary
        entity_resume['RESUME_HASH'] = self.resume_hash_encode(entity_resume)

        return entity_resume

    #---------------------------------------
    def get_resume_dm(self, entity_id):
        sql_stmt = 'select RECORD_COUNT, RESUME_HASH from DM_ENTITY where ENTITY_ID = ?'
        dm_entity_record = self.dbo.fetchRow(self.dbo.sqlExec(sql_stmt, [int(entity_id),]))
        if dm_entity_record:
            return {'ENTITY_ID': entity_id,
                    'RECORD_COUNT': dm_entity_record[0], 
                    'RESUME_HASH': dm_entity_record[1]}
        else:
            return {'ENTITY_ID': entity_id,
                    'RECORD_COUNT': 0, 
                    'RESUME_HASH': ''}

    #---------------------------------------
    def expand_resume_dm(self, dm_entity_resume):
        if dm_entity_resume['RECORD_COUNT'] == 0:
            dm_entity_resume['RECORD_SUMMARY'] = {}
            dm_entity_resume['RELATION_SUMMARY'] = {}
        else:
            #--rebuild dm record and relations summary from hash
            if dm_entity_resume['RESUME_HASH'][0:5] != '~sha~':
                resume_data = self.resume_hash_decode(dm_entity_resume['RESUME_HASH'])
            else:
                resume_data = self.rebuild_resume_dm(dm_entity_resume['ENTITY_ID']) 
            dm_entity_resume['RECORD_SUMMARY'] = resume_data['RECORD_SUMMARY']
            dm_entity_resume['RELATION_SUMMARY'] = resume_data['RELATION_SUMMARY']
        return dm_entity_resume

    #---------------------------------------
    def rebuild_resume_dm(self, entity_id):
        self.log_stat('hash_decode', 'hash(from db)', f'entity_id: {entity_id}')
        resume_data = {'RECORD_SUMMARY': {},
                       'RELATION_SUMMARY': {}}

        sql = 'select DATA_SOURCE, RECORD_ID from DM_RECORD where ENTITY_ID = ?'
        for row in self.dbo.fetchAllRows(self.dbo.sqlExec(sql, [entity_id])):
            if row[0] not in resume_data['RECORD_SUMMARY']:
                resume_data['RECORD_SUMMARY'][row[0]] = []
            resume_data['RECORD_SUMMARY'][row[0]].append(row[1])

        sql = 'select RELATED_ID, MATCH_LEVEL, MATCH_KEY, MATCH_CATEGORY, DATA_SOURCES ' \
              'from DM_RELATION where ENTITY_ID = ?'
        for row in self.dbo.fetchAllRows(self.dbo.sqlExec(sql, entity_id)):
            related_id = str(row[0])
            resume_data['RELATION_SUMMARY'][related_id] = {}
            resume_data['RELATION_SUMMARY'][related_id]['MATCH_LEVEL'] = row[1]
            resume_data['RELATION_SUMMARY'][related_id]['MATCH_KEY'] = row[2]
            resume_data['RELATION_SUMMARY'][related_id]['MATCH_CATEGORY'] = row[3]
            resume_data['RELATION_SUMMARY'][related_id]['DATA_SOURCES'] = self.parse_csv_string(row[4])

        return resume_data

    #----------------------------------------
    def resume_hash_encode(self, entity_resume):
        resume_hash_items = []

        record_count = 0
        for data_source in sorted(entity_resume['RECORD_SUMMARY']):
            resume_hash_items.append('~d~')
            resume_hash_items.append(data_source)
            for record_id in sorted(entity_resume['RECORD_SUMMARY'][data_source]):
                resume_hash_items.append(record_id)
                record_count += 1

        relation_count = 0
        for related_id in sorted(entity_resume['RELATION_SUMMARY']):
            relation_count += 1
            resume_hash_items.append('~r~')
            resume_hash_items.append(related_id)
            resume_hash_items.append(entity_resume['RELATION_SUMMARY'][related_id]['MATCH_LEVEL'])
            resume_hash_items.append(entity_resume['RELATION_SUMMARY'][related_id]['MATCH_KEY'])
            resume_hash_items.append(entity_resume['RELATION_SUMMARY'][related_id]['MATCH_CATEGORY'])
            for data_source in sorted(entity_resume['RELATION_SUMMARY'][related_id]['DATA_SOURCES']):
                resume_hash_items.append(data_source)

        raw_resume_hash = self.make_csv_string(resume_hash_items)

        #--ensure the entity hash can fit in database
        if len(raw_resume_hash) > self.max_resume_hash_len:
            raw_resume_hash_bytes = raw_resume_hash.encode('utf-8')
            zip_resume_hash = zlib.compress(raw_resume_hash_bytes)
            if len(zip_resume_hash) <= self.max_resume_hash_len:
                resume_hash = zip_resume_hash
                compress_method = 'zip'
            else:
                resume_hash = '~sha~' + hashlib.sha256(raw_resume_hash_bytes).hexdigest()                
                compress_method = 'sha'
            self.log_stat('hash_encode', compress_method)

            if self.debug_level > 1:
                self.debug_print(f'compression {len(raw_resume_hash):8} {len(resume_hash):8}  ({compress_method}) records {record_count:5} records {relation_count:5}   {entity_resume["ENTITY_ID"]}')
        else: 
            resume_hash = raw_resume_hash
            self.log_stat('hash_encode', 'str')

        return resume_hash

    #----------------------------------------
    def resume_hash_decode(self, resume_hash):

        if resume_hash[0:1] != '~':
            self.log_stat('hash_decode', 'zip')
            resume_hash = zlib.decompress(resume_hash).decode()
        else:          
            self.log_stat('hash_decode', 'str')

        resume_data = {'RECORD_SUMMARY': {},'RELATION_SUMMARY': {}}

        #--example ['~d~', 'CUSTOMER', '1001', '1002', '1003', '1004', '1005', '~r~', '1001', '2', '+NAME+ADDRESS+DRLIC-DOB', 'PM', 'WATCHLIST', '~r~', '1002', '3', '+ADDRESS+SURNAME', 'PR', 'WATCHLIST']

        resume_hash_items = self.parse_csv_string(resume_hash)
        last_item = len(resume_hash_items) - 1
        current_item = 0
        while True:
            #--parse out a data source and its records
            if resume_hash_items[current_item] == '~d~':
                current_item += 1
                data_source = resume_hash_items[current_item]
                resume_data['RECORD_SUMMARY'][data_source] = []
                current_item += 1

                while True:
                    item_data = resume_hash_items[current_item]
                    if item_data.startswith('~') and item_data.endswith('~') and len(item_data) == 3:
                        break
                    else:
                        resume_data['RECORD_SUMMARY'][data_source].append(item_data)
                        current_item += 1
                        if current_item > last_item:
                            break

            #--parse out a relationship
            elif resume_hash_items[current_item] == '~r~':
                current_item += 1
                related_id = str(resume_hash_items[current_item])
                resume_data['RELATION_SUMMARY'][related_id] = {}

                current_item += 1
                resume_data['RELATION_SUMMARY'][related_id]['MATCH_LEVEL'] = int(resume_hash_items[current_item])
                current_item += 1
                resume_data['RELATION_SUMMARY'][related_id]['MATCH_KEY'] = resume_hash_items[current_item]
                current_item += 1
                resume_data['RELATION_SUMMARY'][related_id]['MATCH_CATEGORY'] = resume_hash_items[current_item]

                resume_data['RELATION_SUMMARY'][related_id]['DATA_SOURCES'] = []
                current_item += 1
                while True:
                    item_data = resume_hash_items[current_item]
                    if item_data.startswith('~') and item_data.endswith('~') and len(item_data) == 3:
                        break
                    else:
                        resume_data['RELATION_SUMMARY'][related_id]['DATA_SOURCES'].append(item_data)
                        current_item += 1
                        if current_item > last_item:
                            break

            #--all done
            if current_item > last_item:
                break

        return resume_data

    #---------------------------------------
    #--interesting entity alerting functions
    #---------------------------------------

    #---------------------------------------
    def process_interesting_entity(self, in_data_source, in_record_id, interesting_entity_data):

        entity_id = interesting_entity_data['ENTITY_ID']
        degrees = interesting_entity_data['DEGREES']
        flags = interesting_entity_data['FLAGS']

        # {
        #     "ENTITY_ID": 1012,
        #     "LENS_CODE": "DEFAULT",
        #     "DEGREES": 0,
        #     "FLAGS": [
        #         "WATCHLIST_CONNECTION"
        #     ],
        #     "SAMPLE_RECORDS": [
        #         {
        #             "FLAGS": [
        #                 "WATCHLIST_CONNECTION"
        #             ],
        #             "DATA_SOURCE": "WATCHLIST",
        #             "RECORD_ID": "1041"
        #         }
        #     ]
        # }

        current_entity_reference = f'entity_id: {entity_id}'
        self.log_stat('interesting_entity', ','.join(flags), current_entity_reference)

        #--(THIS NEEDS TO BE A FINDPATH FROM THE INCOMING ENTITY TO THE INTERESTING ENTITY)
        #--get current entity summary. 
        interesting_resume = self.get_resume_g2_api(entity_id)
        self.debug_print('interesting_resume', interesting_resume)

        alert_list = self.custom_alert_processing(flags, entity_id, interesting_resume)

        alert_dt = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        for alert_record in alert_list:
            entity_id = alert_record['ENTITY_ID']
            alert_reason = alert_record['ALERT_REASON']
            match_level = alert_record['MATCH_LEVEL']

            #--see if entity has already been alerted 
            sql_stmt = 'select ALERT_STATUS, RESUME_HASH from DM_ALERT where ENTITY_ID = ? and ALERT_REASON = ?'
            dm_alert_record = self.dbo.fetchRow(self.dbo.sqlExec(sql_stmt, [int(entity_id), alert_reason]))
            prior_alert_status = dm_alert_record[0] if dm_alert_record else None
            prior_resume_hash = dm_alert_record[1] if dm_alert_record else None

            #--lookup the current resume_hash
            sql_stmt = 'select RESUME_HASH from DM_ENTITY where ENTITY_ID = ?'
            dm_entity_record = self.dbo.fetchRow(self.dbo.sqlExec(sql_stmt, int(entity_id)))
            current_resume_hash = dm_entity_record[0] if dm_entity_record else 'not yet replicated!'

            if not prior_alert_status:
                action = 'insert'
            elif prior_alert_status == 'pending':
                action = 'update' #--still pending, something might have changed
            elif current_resume_hash != prior_resume_hash:
                action = 'insert'
            else:
                #--prior alert has been processed and no change detected!
                action = 'none'

            if action == 'insert':
                sql_stmt = 'insert into DM_ALERT ' + \
                           '(ENTITY_ID, RESUME_HASH, ALERT_REASON, ALERT_STATUS, FIRST_SEEN_DT, LAST_SEEN_DT) ' + \
                           'values (?, ?, ?, ?, ?, ?)'
                stmt_values = [entity_id, current_resume_hash, alert_reason, 'pending', alert_dt, alert_dt]
            else:
                sql_stmt = 'update DM_ALERT set RESUME_HASH = ?, LAST_SEEN_DT = ? ' + \
                           'where ENTITY_ID = ? and ALERT_REASON = ?'
                stmt_values = [current_resume_hash, alert_dt, entity_id, alert_reason]

            try: self.dbo.sqlExec(sql_stmt, stmt_values)
            except Exception as err:
                self.log_stat('sql_error', action + '_alert', f'entity_id: {entity_id}')
                self.debug_print('sql_error', str(err))
                self.replication_status = 2 #--sql error

    #---------------------------------------
    #--supporting functions
    #---------------------------------------

    #----------------------------------------
    def make_csv_string(self, list_data):
        csv_output = io.StringIO()
        csv.writer(csv_output, quoting=csv.QUOTE_MINIMAL).writerow(list_data)
        return csv_output.getvalue()[0:-2] #--removes \r\n

    #----------------------------------------
    def parse_csv_string(self, csv_string):
        return list(csv.reader([csv_string]))[0]

    #----------------------------------------
    def log_stat(self, cat1, cat2, ref_data=''):
        if cat1 not in self.stat_log:
            self.stat_log[cat1] = {}
        if cat2 not in self.stat_log[cat1]:
            self.stat_log[cat1][cat2] = 1
        else:
            self.stat_log[cat1][cat2] += 1

        if self.debug_level or cat1 == 'sql_error':
            self.debug_print(cat1, cat2, ref_data)

    #----------------------------------------
    def debug_print(self, *argv):
        if not (self.debug_level or 'sql_error' in argv):
            return
        if len(argv) > 0:
            ref_data = argv[-1]
            if type(ref_data) == dict:
                if self.debug_level > 1:
                    other_info = ' | '.join([str(x) for x in argv[0:-1]])
                    print('-- ' + other_info + ' --')
                    try: print(json.dumps(ref_data, indent=4))
                    except: print('could not dump to json string!') 
            else: 
                other_info = ' | '.join([str(x) for x in argv])
                print(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), other_info)
        else:
            print()

#-----------------------------
#-- custom replication class here for testing only 
#-----------------------------

#----------------------------------------
if __name__ == "__main__":

    #--defaults
    try: iniFileName = G2Paths.get_G2Module_ini_path()
    except: iniFileName = '' 

    #--capture the command line arguments
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-c', '--config_file_name', dest='iniFileName', default=iniFileName, help='name of the g2.ini file, defaults to %s' % iniFileName)
    arg_parser.add_argument('-e', '--entity_list', dest='entity_list', help='list of entity_ids to test or all')
    arg_parser.add_argument('-d', '--data_source', dest='data_source', default=None, help='data_source to use for all')
    arg_parser.add_argument('-P', '--purge', dest='purge', action='store_true', default=False, help='purge datamart first')
    arg_parser.add_argument('-D', '--debug', dest='debug', type=int, default=0, help='debug level 1=normal 2 includes json')
    args = arg_parser.parse_args()

    #--get parameters from ini file
    if not os.path.exists(args.iniFileName):
        print('\nAn ini file was not found, please supply with the -c parameter\n')
        sys.exit(1)

    # Get the INI paramaters to use
    iniParamCreator = G2IniParams()
    g2module_params = iniParamCreator.getJsonINIParams(args.iniFileName)
    datamart_connection_uri = json.loads(g2module_params)['DATAMART']['CONNECTION']
    try: dm_replicator = Replicator(args.iniFileName, None, datamart_connection_uri, debug_level=args.debug)
    except Exception as err:
        print('\n%s\n' % err)
        sys.exit(1)

    print('\nsuccessfully initialized!')

    if args.entity_list:
        if args.purge:
            print('\n** purging data mart first **\n')
            dm_replicator.dbo.sqlExec('delete from DM_ENTITY')
            dm_replicator.dbo.sqlExec('delete from DM_RECORD')
            dm_replicator.dbo.sqlExec('delete from DM_RELATION')
            dm_replicator.dbo.sqlExec('delete from DM_REPORT')
            dm_replicator.dbo.sqlExec('delete from DM_REPORT_DETAIL')

        if args.entity_list.upper().startswith('ALL'):
            dbUri = json.loads(g2module_params)['SQL']['CONNECTION']
            try: 
                g2dbo = G2Database(dbUri)
            except Exception as err:
                print(err)
                sys.exit(1)

            started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if args.entity_list.upper() == 'ALL' and args.data_source:
                sql = 'SELECT ' \
                      ' A.DSRC_ID, ' \
                      ' D.CODE AS DATA_SOURCE, ' \
                      ' A.RECORD_ID, ' \
                      ' C.RES_ENT_ID ' \
                      'FROM DSRC_RECORD A ' \
                      'JOIN OBS_ENT B ON B.ENT_SRC_KEY = A.ENT_SRC_KEY AND B.DSRC_ID = A.DSRC_ID AND B.ETYPE_ID = A.ETYPE_ID ' \
                      'JOIN RES_ENT_OKEY C ON C.OBS_ENT_ID = B.OBS_ENT_ID ' \
                      "LEFT JOIN SYS_CODES_USED D ON D.CODE_ID = A.DSRC_ID and D.CODE_TYPE = 'DATA_SOURCE'"
            else:
                sql = 'SELECT RES_ENT_ID FROM RES_ENT'

            print()
            cursor1 = g2dbo.sqlExec(sql)
            row = g2dbo.fetchRow(cursor1)
            cnt = 0
            while row:
                cnt += 1
                if args.entity_list.upper() == 'ALL' and args.data_source:
                    json_msg = {'DATA_SOURCE': row[1],
                                'RECORD_ID': row[2],
                                'AFFECTED_ENTITIES': [{'ENTITY_ID': row[3]}]}
                    dm_replicator.replicate(json.dumps(json_msg))
                else:
                    dm_replicator.replicate_entity(row[0], 'user-request')
                row = g2dbo.fetchRow(cursor1)
                if cnt % 1000 == 0 or not row:
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(('%s entities replicated at %s' % (cnt, now)) + (', complete!' if not row else ''))
                if cnt == 1000000:
                    break

            ended = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f'\nStarted: {started}')
            print(f'  Ended: {ended}\n')

        #--resynch entity on demand (resynchs the entire entity, not each record)
        else:
            for entity_id in args.entity_list.split(','):
                dm_replicator.replicate_entity(int(entity_id), 'user-request')

        print('\n-- PROCESSING STATS ---------------------------------')
        print(json.dumps(dm_replicator.stat_log, indent=4))

        print('\n-- REPORT STATS -------------------------------------')
        for record in dm_replicator.dbo.fetchAllDicts(dm_replicator.dbo.sqlExec('select * from DM_REPORT order by REPORT, DATA_SOURCE1, DATA_SOURCE2, STATISTIC')):
            print(record['REPORT'], record['STATISTIC'], record['DATA_SOURCE1'], record['DATA_SOURCE2'], record['RECORD_COUNT'], record['ENTITY_COUNT'], record['RELATION_COUNT'])
    print()
    sys.exit(0)
