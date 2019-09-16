#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys
import traceback
from datetime import datetime

from decimal import Decimal

from clickhouse_mysql.dbclient.chclient import CHClient

from clickhouse_mysql.writer.writer import Writer
from clickhouse_mysql.event.event import Event


class CHWriter(Writer):
    """ClickHouse writer"""

    client = None
    dst_schema = None
    dst_table = None
    dst_distribute = None

    def __init__(
            self,
            connection_settings,
            dst_schema=None,
            dst_table=None,
            dst_distribute=False,
            next_writer_builder=None,
            converter_builder=None,
    ):
        if dst_distribute and dst_schema is not None:
            dst_schema += "_all"
        if dst_distribute and dst_table is not None:
            dst_table += "_all"
        logging.info("CHWriter() connection_settings={} dst_schema={} dst_table={} dst_distribute={}".format(connection_settings, dst_schema, dst_table, dst_distribute))
        self.client = CHClient(connection_settings)
        self.dst_schema = dst_schema
        self.dst_table = dst_table
        self.dst_distribute = dst_distribute

    def insert(self, event_or_events=None,fs = {}):
        # event_or_events = [
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        # ]

        events = self.listify(event_or_events)
        if len(events) < 1:
            logging.warning('No events to insert. class: %s', __class__)
            return

        # assume we have at least one Event

        logging.debug('class:%s insert %d event(s)', __class__, len(events))

        # verify and converts events and consolidate converted rows from all events into one batch

        rows = []
        event_converted = None
        for event in events:
            if not event.verify:
                logging.warning('Event verification failed. Skip one event. Event: %s Class: %s', event.meta(), __class__)
                continue # for event

            event_converted = self.convert(event)
            dt = datetime.strptime('2038-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
            dm = datetime.strptime('1970-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
            for row in event_converted:
                for key in row.keys():
                    # we need to convert Decimal value to str value for suitable for table structure
                  #  if (type(row[key]) == Decimal):
                     #   row[key] = str(row[key])

                    if key in fs:
                        if "datetime" in fs[key]["type"] and row[key] is not None :
                            if row[key] > dt or row[key] < dm:
                                logging.debug("datetime rewriting: %s %s",key,row[key])
                                if bool(fs[key]["null"]):
                                    row[key] = None
                                else:
                                    row[key] = datetime.strptime('1970-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
                                logging.debug("datetime rewrite to: %s %s",key,row[key])
                        if row[key] is None:
                            if "datetime" in fs[key]["type"] and (key == "created_at" or key == "updated_at"):
                                row[key] = datetime.strptime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
                            if "int" in fs[key]["type"] and ("_id" in key):
                                row[key] = 0

                rows.append(row)

        logging.debug('class:%s insert %d row(s)', __class__, len(rows))

        # determine target schema.table

        schema = self.dst_schema if self.dst_schema else event_converted.schema
        table = None
        if self.dst_table:
            table = self.dst_table
        elif self.dst_distribute:
            # if current is going to insert distributed table,we need '_all' suffix
            table = event_converted.schema + "__" + event_converted.table + "_all"
        else:
            table = event_converted.table
        logging.debug("schema={} table={} self.dst_schema={} self.dst_table={}".format(schema, table, self.dst_schema, self.dst_table))

        # and INSERT converted rows

        sql = ''
        try:
            sql = 'INSERT INTO `{0}`.`{1}` ({2}) VALUES'.format(
                schema,
                table,
                ', '.join(map(lambda column: '`%s`' % column, rows[0].keys()))
            )
            self.client.execute(sql, rows)
        except Exception as ex:
            logging.critical('QUERY FAILED')
            logging.critical('ex={}'.format(ex))
            logging.critical('sql={}'.format(sql))
            logging.debug('=============')
            traceback.print_exc()
            logging.debug('=============')
            print(ex)
            sys.exit(0)

        # all DONE



if __name__ == '__main__':
    connection_settings = {
        'host': '192.168.74.230',
        'port': 9000,
        'user': 'default',
        'passwd': '',
    }

    writer = CHWriter(connection_settings=connection_settings)
    writer.insert()
