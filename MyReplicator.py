import G2Replicator

class Replicator(G2Replicator.Replicator):

    #---------------------------------------
    def __init__(self, iniFileName, g2Engine, datamartConnectionStr, **kwargs):
        super().__init__(iniFileName, g2Engine, datamartConnectionStr, **kwargs)
        self.custom_entity_fields = True
        self.custom_record_fields = True
        self.custom_relation_fields = False
        self.custom_alert_processor = True

    #---------------------------------------
    def custom_dm_entity_fields(self, entity_summary):
        custom_fields = []
        custom_values = []

        if 'CUSTOMER' in entity_summary['RECORD_SUMMARY']:
            custom_fields.append('CUSTOMER_COUNT')
            custom_values.append(len(entity_summary['RECORD_SUMMARY']['CUSTOMER']))

        if 'WATCHLIST' in entity_summary['RECORD_SUMMARY']:
            custom_fields.append('WATCHLIST_COUNT')
            custom_values.append(len(entity_summary['RECORD_SUMMARY']['WATCHLIST']))

        return custom_fields, custom_values

    #---------------------------------------
    def custom_dm_record_fields(self, data_source, record_id, json_data):

        custom_fields = []
        custom_values = []

        if data_source == 'CUSTOMER':
            custom_fields, custom_values = self.custom_customer_fields(data_source, record_id, json_data)

        if data_source == 'WATCHLIST':
            custom_fields, custom_values = self.custom_watchlist_fields(data_source, record_id, json_data)

        return custom_fields, custom_values

    #---------------------------------------
    def custom_customer_fields(self, data_source, record_id, json_data):
        custom_fields = []
        custom_values = []

        #--parse the json record
        full_name = json_data['PRIMARY_NAME_LAST'] if 'PRIMARY_NAME_LAST' in json_data else ''
        if 'PRIMARY_NAME_FIRST' in json_data:
            full_name += (', ' + json_data['PRIMARY_NAME_FIRST'])
        if 'PRIMARY_NAME_MIDDLE' in json_data:
            full_name += (' ' + json_data['PRIMARY_NAME_MIDDLE'])
        key_date = json_data['DATE'] if 'DATE' in json_data and json_data['DATE'] else ''
        key_status = json_data['STATUS'] if 'STATUS' in json_data and json_data['STATUS'] else ''
        key_amount = int(float(json_data['AMOUNT']) * 100) \
            if 'AMOUNT' in json_data and json_data['AMOUNT'] else ''

        #--add to DM_RECORD
        custom_fields.append('PRIMARY_NAME')
        custom_values.append(full_name)
        custom_fields.append('KEY_DATE')
        custom_values.append(key_date)
        custom_fields.append('KEY_STATUS')
        custom_values.append(key_status)
        custom_fields.append('KEY_AMOUNT')
        custom_values.append(key_amount)

        #--or update your own table
        sql = 'insert into CUSTOMER ' + \
              '(CUSTOMER_ID, PRIMARY_NAME, SINCE_DATE, STATUS, AMOUNT, DATA_SOURCE, RECORD_ID) ' + \
              'values (?, ?, ?, ?, ?, ?, ?) ' + \
              ' on conflict (CUSTOMER_ID) do update set ' + \
              'PRIMARY_NAME = ?, SINCE_DATE = ?, STATUS = ?, AMOUNT = ?'
        values = [record_id, full_name, key_date, key_status, key_amount, data_source, record_id,
                  full_name, key_date, key_status, key_amount]
        try: db_response = self.dbo.sqlExec(sql, values)
        except Exception as err:
            self.log_stat('sql_error', 'upsert_customer', f'customer_id: {record_id}')
            self.debug_print('\t' + str(err))
            self.replication_status = 2 #--sql error
            raise err
        else:
            self.log_stat('custom', 'upsert_customer', f'customer_id: {record_id}')

        return custom_fields, custom_values

    #---------------------------------------
    def custom_watchlist_fields(self, data_source, record_id, json_data):
        custom_fields = []
        custom_values = []

        #--parse the json record
        full_name = json_data['PRIMARY_NAME_LAST'] if 'PRIMARY_NAME_LAST' in json_data else ''
        if 'PRIMARY_NAME_FIRST' in json_data:
            full_name += (', ' + json_data['PRIMARY_NAME_FIRST'])
        if 'PRIMARY_NAME_MIDDLE' in json_data:
            full_name += (' ' + json_data['PRIMARY_NAME_MIDDLE'])
        key_date = json_data['DATE'] if 'DATE' in json_data and json_data['DATE'] else ''
        key_status = json_data['STATUS'] if 'STATUS' in json_data and json_data['STATUS'] else ''
        key_category = json_data['CATEGORY'] if 'CATEGORY' in json_data and json_data['CATEGORY'] else ''

        #--add to DM_RECORD
        custom_fields.append('PRIMARY_NAME')
        custom_values.append(full_name)
        custom_fields.append('KEY_DATE')
        custom_values.append(key_date)
        custom_fields.append('KEY_STATUS')
        custom_values.append(key_status)
        custom_fields.append('KEY_CATEGORY')
        custom_values.append(key_category)

        #--or update your own table
        sql = 'insert into WATCHLIST ' + \
              '(ENTRY_ID, PRIMARY_NAME, PUBLISH_DATE, STATUS, CATEGORY, DATA_SOURCE, RECORD_ID) ' + \
              'values (?, ?, ?, ?, ?, ?, ?) ' + \
              ' on conflict (ENTRY_ID) do update set ' + \
              'PRIMARY_NAME = ?, PUBLISH_DATE = ?, STATUS = ?, CATEGORY = ?'
        values = [record_id, full_name, key_date, key_status, key_category, data_source, record_id,
                  full_name, key_date, key_status, key_category]
        try: db_response = self.dbo.sqlExec(sql, values)
        except Exception as err:
            self.log_stat('sql_error', 'upsert_watchlist', f'entry_id: {record_id}')
            self.debug_print('\t' + str(err))
            self.replication_status = 2 #--sql error
            raise err
        else:
            self.log_stat('custom', 'upsert_watchlist', f'entry_id: {record_id}')

        return custom_fields, custom_values

    #---------------------------------------
    def custom_alert_processing(self, flags, entity_id, interesting_resume):
        alert_list = []

        if 'WATCHLIST_CONNECTION' in flags:
            entity_on_watchlist = 'WATCHLIST' in interesting_resume['RECORD_SUMMARY']
            for data_source in interesting_resume['RECORD_SUMMARY']:
                if data_source != 'WATCHLIST':
                    alert_list.append({'ENTITY_ID': entity_id, 
                                       'ALERT_REASON': f'WATCHLIST|{data_source}',
                                       'MATCH_LEVEL': 'IS'})
            if entity_on_watchlist:
                for related_id in interesting_resume['RELATION_SUMMARY']:
                    for data_source in interesting_resume['RELATION_SUMMARY'][related_id]['DATA_SOURCES']:
                        if data_source != 'WATCHLIST':
                            alert_list.append({'ENTITY_ID': related_id, 
                                               'ALERT_REASON': f'WATCHLIST|{data_source}',
                                               'MATCH_LEVEL': interesting_resume['RELATION_SUMMARY'][related_id]['MATCH_CATEGORY']})
        return alert_list

