#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import logging
import sys
import traceback
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from MySQLdb.cursors import SSDictCursor,Cursor
from clickhouse_mysql.reader.reader import Reader
from clickhouse_mysql.event.event import Event
from clickhouse_mysql.tableprocessor import TableProcessor
from clickhouse_mysql.util import Util
from clickhouse_mysql.dbclient.mysqlclient import MySQLClient
#from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent

table_schema_cache = {}
table_schema_cache_time = {}

last_binlog_pos = None
last_flush_time = 0

class FakeRows:
    rows = []
    schema = None
    table = None

class CachePool():
    caches = {}
    last_full_check = 0
    reader = None
    def push_event(self,schema,table,rows,file,pos):
        cache_key = "{}.{}".format(schema,table)
        if cache_key not in self.caches:
            c = EventCache()
            c.schema = schema
            c.table = table
            c.reader = self.reader
            self.caches[cache_key] = c
        self.caches[cache_key].push_event(rows,file,pos)

    def loop(self):
        now = time.time()
        if now - self.last_full_check > 60:
            for k in self.caches:
                c = self.caches[k]
                if c.last_flush_time > 60 and len(c.events) > 0:
                    c.flush({})
            self.last_full_check = now


class EventCache():
    events = []
    last_binlog_file_name = None
    last_binlog_file_pos = 0
    schema = None
    table = None
    column_count = None
    last_flush_time = 0
    reader = None

    def push_event(self,event,file,pos):
        now = time.time()
        #if dismatched column,will syncing the cached data first
        if self.column_count and self.column_count != len(event.rows[0]['values']) and len(self.events) > 0:
            self.flush(event)
            self.column_count = len(event.rows[0]['values'])

        self.events = self.events + event.rows
        self.column_count = len(event.rows[0]['values'])
        self.last_binlog_file_name = file
        self.last_binlog_file_pos = pos
        if len(self.events) > 100 or now - self.last_flush_time > 60:
            self.flush(event)

    def flush(self,event):
        logging.info("start flush schema:{} table:{} rows:{}",self.schema,self.table,len(self.events))
        batch_event = FakeRows()
        batch_event.rows = self.events
        event = Event()
        event.schema = self.schema
        event.table = self.table
        event.pymysqlreplication_event = batch_event
        event.fs = self.reader.get_field_schema_cache(self.schema,self.table)
        self.reader.process_first_event(event=event)
        self.reader.notify('WriteRowsEvent', event=event)
        self.events = []
        self.last_flush_time = time.time()
        self.reader.process_binlog_position(self.last_binlog_file_name,self.last_binlog_file_pos)





class MySQLReader(Reader):
    """Read data from MySQL as replication ls"""

    connection_settings = None
    server_id = None
    log_file = None
    log_pos = None
    schemas = None
    tables = None
    tables_prefixes = None
    blocking = None
    resume_stream = None
    binlog_stream = None
    nice_pause = 0

    write_rows_event_num = 0
    write_rows_event_each_row_num = 0;

    binlog_position_file = None
    cache_pool = None

    def __init__(
            self,
            connection_settings,
            server_id,
            log_file=None,
            log_pos=None,
            schemas=None,
            tables=None,
            tables_prefixes=None,
            blocking=None,
            resume_stream=None,
            nice_pause=None,
            binlog_position_file=None,
            callbacks={},
    ):
        super().__init__(callbacks=callbacks)

        self.connection_settings = connection_settings
        self.server_id = server_id
        self.log_file = log_file
        self.log_pos = log_pos
        self.schemas = None if not TableProcessor.extract_dbs(schemas, Util.join_lists(tables, tables_prefixes)) else TableProcessor.extract_dbs(schemas, Util.join_lists(tables, tables_prefixes))
        self.tables = None if tables is None else TableProcessor.extract_tables(tables)
        self.tables_prefixes = None if tables_prefixes is None else TableProcessor.extract_tables(tables_prefixes)
        self.blocking = blocking
        self.resume_stream = resume_stream
        self.nice_pause = nice_pause
        self.binlog_position_file=binlog_position_file
        self.cache_pool = CachePool()
        self.cache_pool.reader = self

        logging.info("raw dbs list. len()=%d", 0 if schemas is None else len(schemas))
        if schemas is not None:
            for schema in schemas:
                logging.info(schema)
        logging.info("normalised dbs list. len()=%d", 0 if self.schemas is None else len(self.schemas))
        if self.schemas is not None:
            for schema in self.schemas:
                logging.info(schema)

        logging.info("raw tables list. len()=%d", 0 if tables is None else len(tables))
        if tables is not None:
            for table in tables:
                logging.info(table)
        logging.info("normalised tables list. len()=%d", 0 if self.tables is None else len(self.tables))
        if self.tables is not None:
            for table in self.tables:
                logging.info(table)

        logging.info("raw tables-prefixes list. len()=%d", 0 if tables_prefixes is None else len(tables_prefixes))
        if tables_prefixes is not None:
            for table in tables_prefixes:
                logging.info(table)
        logging.info("normalised tables-prefixes list. len()=%d", 0 if self.tables_prefixes is None else len(self.tables_prefixes))
        if self.tables_prefixes is not None:
            for table in self.tables_prefixes:
                logging.info(table)

        if not isinstance(self.server_id, int):
            raise Exception("Please specify server_id of src server as int. Ex.: --src-server-id=1")

        self.binlog_stream = BinLogStreamReader(
            # MySQL server - data source
            connection_settings=self.connection_settings,
            server_id=self.server_id,
            # we are interested in reading CH-repeatable events only
            only_events=[
                # Possible events
                #BeginLoadQueryEvent,
                DeleteRowsEvent,
                #ExecuteLoadQueryEvent,
                #FormatDescriptionEvent,
                #GtidEvent,
                #HeartbeatLogEvent,
                #IntvarEvent
                #NotImplementedEvent,
                #QueryEvent,
                #RotateEvent,
                #StopEvent,
                #TableMapEvent,
                UpdateRowsEvent,
                WriteRowsEvent,
                #XidEvent,
            ],
            only_schemas=self.schemas,
            # in case we have any prefixes - this means we need to listen to all tables within specified schemas
            only_tables=self.tables if not self.tables_prefixes else None,
            log_file=self.log_file,
            log_pos=self.log_pos,
            freeze_schema=True, # If true do not support ALTER TABLE. It's faster.
            blocking=False,
            resume_stream=self.resume_stream,
        )
        logging.debug("mysql connection settings:{}".format(self.connection_settings))

    def performance_report(self, start, rows_num, rows_num_per_event_min=None, rows_num_per_event_max=None, now=None):
        # log performance report

        if now is None:
            now = time.time()

        window_size = now - start
        if window_size > 0:
            rows_per_sec = rows_num / window_size
            logging.info(
                'PERF - %f rows/sec, min(rows/event)=%d max(rows/event)=%d for last %d rows %f sec',
                rows_per_sec,
                rows_num_per_event_min if rows_num_per_event_min is not None else -1,
                rows_num_per_event_max if rows_num_per_event_max is not None else -1,
                rows_num,
                window_size,
            )
        else:
            logging.info("PERF - can not calc performance for time size=0")

    def is_table_listened(self, table):
        """
        Check whether table name in either directly listed in tables or starts with prefix listed in tables_prefixes
        :param table: table name
        :return: bool is table listened
        """

        # check direct table name match
        if self.tables:
            if table in self.tables:
                return True

        # check prefixes
        if self.tables_prefixes:
            for prefix in self.tables_prefixes:
                if table.startswith(prefix):
                    # table name starts with prefix list
                    return True

        return False

    first_rows_passed = []
    start_timestamp = 0
    start = 0
    rows_num = 0
    rows_num_since_interim_performance_report = 0
    rows_num_per_event_min = None
    rows_num_per_event_max = None

    def init_read_events(self):
        self.start_timestamp = int(time.time())
        self.first_rows_passed = []

    def init_fetch_loop(self):
        self.start = time.time()

    def stat_init_fetch_loop(self):
        self.rows_num = 0
        self.rows_num_since_interim_performance_report = 0
        self.rows_num_per_event_min = None
        self.rows_num_per_event_max = None

    def stat_close_fetch_loop(self):
        if self.rows_num > 0:
            # we have some rows processed
            now = time.time()
            if now > self.start + 60:
                # and processing was long enough
                self.performance_report(self.start, self.rows_num, now)

    def stat_write_rows_event_calc_rows_num_min_max(self, rows_num_per_event):
        # populate min value
        if (self.rows_num_per_event_min is None) or (rows_num_per_event < self.rows_num_per_event_min):
            self.rows_num_per_event_min = rows_num_per_event

        # populate max value
        if (self.rows_num_per_event_max is None) or (rows_num_per_event > self.rows_num_per_event_max):
            self.rows_num_per_event_max = rows_num_per_event

    def stat_write_rows_event_all_rows(self, mysql_event):
        self.write_rows_event_num += 1
        self.rows_num += len(mysql_event.rows)
        self.rows_num_since_interim_performance_report += len(mysql_event.rows)
        logging.debug('WriteRowsEvent #%d rows: %d', self.write_rows_event_num, len(mysql_event.rows))

    def stat_write_rows_event_each_row(self):
        self.write_rows_event_each_row_num += 1
        logging.debug('WriteRowsEvent.EachRow #%d', self.write_rows_event_each_row_num)

    def stat_write_rows_event_each_row_for_each_row(self):
        self.rows_num += 1
        self.rows_num_since_interim_performance_report += 1

    def stat_write_rows_event_finalyse(self):
        if self.rows_num_since_interim_performance_report >= 100000:
            # speed report each N rows
            self.performance_report(
                start=self.start,
                rows_num=self.rows_num,
                rows_num_per_event_min=self.rows_num_per_event_min,
                rows_num_per_event_max=self.rows_num_per_event_max,
            )
            self.rows_num_since_interim_performance_report = 0
            self.rows_num_per_event_min = None
            self.rows_num_per_event_max = None

    def process_first_event(self, event):
        if "{}.{}".format(event.schema, event.table) not in self.first_rows_passed:
            Util.log_row(event.first_row(), "first row in replication {}.{}".format(event.schema, event.table))
            self.first_rows_passed.append("{}.{}".format(event.schema, event.table))
     #   logging.info(self.first_rows_passed)

    def get_field_schema_cache(self,db,table):
        global table_schema_cache_time
        global table_schema_cache
        cache_key = db + '_' + table

        fs = {}
        has_cache = False
        if cache_key in table_schema_cache_time:
            if time.time() - table_schema_cache_time[cache_key] > 3600 * 6:
                has_cache = False
            else:
                has_cache = True

        if not has_cache:
            logging.debug("start reflash table:" + cache_key)
            ret = self.get_columns(db,table)
            fs = ret[0]
            table_schema_cache_time[cache_key] = time.time()
            table_schema_cache[cache_key] = fs
        else:
            fs = table_schema_cache[cache_key]
        return fs

    def process_write_rows_event(self, mysql_event,file,pos):
        """
        Process specific MySQL event - WriteRowsEvent
        :param mysql_event: WriteRowsEvent instance
        :return:
        """
        if self.tables_prefixes:
            # we have prefixes specified
            # need to find whether current event is produced by table in 'looking-into-tables' list
            if not self.is_table_listened(mysql_event.table):
                # this table is not listened
                # processing is over - just skip event
                return


        self.cache_pool.push_event(mysql_event.schema,mysql_event.table,mysql_event,file,pos)

        return


        fs = self.get_field_schema_cache(mysql_event.schema,mysql_event.table)

        # statistics
        self.stat_write_rows_event_calc_rows_num_min_max(rows_num_per_event=len(mysql_event.rows))

        if self.subscribers('WriteRowsEvent'):
            # dispatch event to subscribers

            # statistics
            self.stat_write_rows_event_all_rows(mysql_event=mysql_event)

            # dispatch Event
            event = Event()
            event.schema = mysql_event.schema
            event.table = mysql_event.table
            event.pymysqlreplication_event = mysql_event
            event.fs = fs

            self.process_first_event(event=event)
            self.notify('WriteRowsEvent', event=event)

        if self.subscribers('WriteRowsEvent.EachRow'):
            # dispatch event to subscribers

            # statistics
            self.stat_write_rows_event_each_row()

            # dispatch Event per each row
            for row in mysql_event.rows:
                # statistics
                self.stat_write_rows_event_each_row_for_each_row()

                # dispatch Event
                event = Event()
                event.schema = mysql_event.schema
                event.table = mysql_event.table
                event.row = row['values']
                event.fs = fs
                self.process_first_event(event=event)
                self.notify('WriteRowsEvent.EachRow', event=event)

        self.stat_write_rows_event_finalyse()

    def process_update_rows_event(self, mysql_event,file,pos):
        if self.tables_prefixes:
            # we have prefixes specified
            # need to find whether current event is produced by table in 'looking-into-tables' list
            if not self.is_table_listened(mysql_event.table):
                # this table is not listened
                # processing is over - just skip event
                return
        for row in mysql_event.rows:
            row["values"] = row["after_values"]

        self.cache_pool.push_event(mysql_event.schema,mysql_event.table,mysql_event,file,pos)

        return


        # statistics
        self.stat_write_rows_event_calc_rows_num_min_max(rows_num_per_event=len(mysql_event.rows))

        fs = self.get_field_schema_cache(mysql_event.schema,mysql_event.table)

        if self.subscribers('UpdateRowsEvent'):
            logging.debug("start update")
        # dispatch event to subscribers

        # statistics
            self.stat_write_rows_event_all_rows(mysql_event=mysql_event)

        # dispatch Event
            event = Event()
            event.schema = mysql_event.schema
            event.table = mysql_event.table
            event.pymysqlreplication_event = mysql_event
            event.fs = fs

            self.process_first_event(event=event)
            self.notify('UpdateRowsEvent', event=event)

        if self.subscribers('UpdateRowsEvent.EachRow'):
            # dispatch event to subscribers

            # statistics
            self.stat_write_rows_event_each_row()

            # dispatch Event per each row
            for row in mysql_event.rows:
                # statistics
                self.stat_write_rows_event_each_row_for_each_row()
                # dispatch Event
                event = Event()
                event.schema = mysql_event.schema
                event.table = mysql_event.table
                event.row = row['values']
                event.before_row = row["before_values"]
                event.fs = fs
                self.process_first_event(event=event)
                self.notify('UpdateRowsEvent.EachRow', event=event)

        self.stat_write_rows_event_finalyse()



    def process_delete_rows_event(self, mysql_event):
        logging.info("Skip delete rows")

    def process_binlog_position(self, file, pos):
        global last_binlog_pos
        global last_flush_time
        new_binlog_pos = "{}:{}".format(file, pos)
        logging.info("start process binlog position:{}".format(new_binlog_pos))
        if last_binlog_pos is None or file > last_binlog_pos:
            last_binlog_pos = new_binlog_pos
        else:
            if new_binlog_pos > last_binlog_pos:
                last_binlog_pos = new_binlog_pos
            else:
                logging.info("skip process binlog position: stored: {},new: {}".format(last_binlog_pos,new_binlog_pos))
                return
        now = time.time()
        if self.binlog_position_file and now - last_flush_time > 120:
            with open(self.binlog_position_file, "w") as f:
                f.write(last_binlog_pos)
                last_flush_time = now
        logging.debug("Next event binlog pos: {}.{}".format(file, pos))

    def read(self):
        # main function - read data from source

        self.init_read_events()

        # fetch events
        try:
            while True:
                logging.debug('Check events in binlog stream')
                self.cache_pool.loop()

                self.init_fetch_loop()

                # statistics
                self.stat_init_fetch_loop()

                try:
                    # fetch available events from MySQL
                    for mysql_event in self.binlog_stream:
                        # new event has come
                        # check what to do with it

                        # process event based on its type
                        if isinstance(mysql_event, WriteRowsEvent):
                            self.process_write_rows_event(mysql_event,self.binlog_stream.log_file, self.binlog_stream.log_pos)
                        elif isinstance(mysql_event, DeleteRowsEvent):
                            self.process_delete_rows_event(mysql_event)
                        elif isinstance(mysql_event, UpdateRowsEvent):
                            self.process_update_rows_event(mysql_event,self.binlog_stream.log_file, self.binlog_stream.log_pos)
                        else:
                            # skip other unhandled events
                            pass

                        # after event processed, we need to handle current binlog position
                       # self.process_binlog_position(self.binlog_stream.log_file, self.binlog_stream.log_pos)

                except KeyboardInterrupt:
                    # pass SIGINT further
                    logging.info("SIGINT received. Pass it further.")
                    raise
                except Exception as ex:
                    if self.blocking:
                        # we'd like to continue waiting for data
                        # report and continue cycle
                        logging.warning("Got an exception, skip it in blocking mode")
                        logging.warning(ex)
                        traceback.print_exc(file=sys.stdout)
                        sys.exit(1)

                    else:
                        # do not continue, report error and exit
                        logging.critical("Got an exception, abort it in non-blocking mode")
                        logging.critical(ex)
                        traceback.print_exc(file=sys.stdout)
                        sys.exit(1)

                # all events fetched (or none of them available)

                # statistics
                self.stat_close_fetch_loop()

                if not self.blocking:
                    # do not wait for more data - all done
                    break # while True

                # blocking - wait for more data
                if self.nice_pause > 0:
                    time.sleep(self.nice_pause)

                self.notify('ReaderIdleEvent')

        except KeyboardInterrupt:
            logging.info("SIGINT received. Time to exit.")
        except Exception as ex:
            logging.warning("Got an exception, handle it")
            logging.warning(ex)

        try:
            self.binlog_stream.close()
        except Exception as ex:
            logging.warning("Unable to close binlog stream correctly")
            logging.warning(ex)

        end_timestamp = int(time.time())

        logging.info('start %d', self.start_timestamp)
        logging.info('end %d', end_timestamp)
        logging.info('len %d', end_timestamp - self.start_timestamp)


    def get_columns(self,db,full_table_name):
        connection_settings = self.connection_settings
        connection_settings["password"] = connection_settings['passwd']
        logging.debug("mysql connection settings:{}".format(connection_settings))
        client = MySQLClient(connection_settings)
        client.cursorclass = Cursor
        client.connect(db=db)
        client.cursor.execute("DESC {}".format(full_table_name))
        fields = []
        fs = {}
        for (_field, _type, _null, _key, _default, _extra,) in client.cursor:
            fields.append("`" +_field + "`")
            fs[_field] = {
                "field" : _field,
                "type" : _type,
                "null" : _null,
                "default" : _default,
            }
        client.disconnect()
        return [fs,fields]
if __name__ == '__main__':
    connection_settings = {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'reader',
        'passwd': 'qwerty',
    }
    server_id = 1

    reader = Reader(
        connection_settings=connection_settings,
        server_id=server_id,
    )

    reader.read()
