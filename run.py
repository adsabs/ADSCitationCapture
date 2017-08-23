#!/usr/bin/env python
import os
import sys
import tempfile
import argparse
import json
from adscc import tasks

from adsputils import setup_logging
logger = setup_logging('run.py')

import postgres_copy
from datetime import datetime
from sqlalchemy.schema import CreateSchema
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import create_engine
from adscc.models import build_raw_citation_table
from adsputils import load_config
config = load_config()

def verify_input_data(connection, schema_name, expanded_table_name):
    ## Check assumptions
    # - At least one field contains a value for doi, pid or url
    count_all_fields_null_sql = \
            "select count(*) \
                from {0}.{1} \
                where (\
                        doi is null \
                        and pid is null \
                        and url is null \
                    );"
    sql_command = count_all_fields_null_sql.format(schema_name, expanded_table_name)
    logger.debug("Executing SQL: %s", sql_command)
    count_all_fields_null = connection.execute(sql_command).scalar()

    if count_all_fields_null > 0:
        raise Exception("There is at least an entry with all doi, pid and url fields set to null")

    # - Only one field contains a value for doi, pid or url
    count_too_many_fields_not_null_sql = \
            "select count(*) \
                from {0}.{1} \
                where (\
                        (doi is not null and pid is not null and url is null) \
                        or (doi is not null and pid is null and url is not null) \
                        or (doi is null and pid is not null and url is not null) \
                        or (doi is not null and pid is not null and url is not null) \
                    );"
    sql_command = count_too_many_fields_not_null_sql.format(schema_name, expanded_table_name)
    logger.debug("Executing SQL: %s", sql_command)
    count_too_many_fields_not_null = connection.execute(sql_command).scalar()

    if count_too_many_fields_not_null > 0:
        raise Exception("There is at least an entry with two or more doi, pid and url fields set to a value")

    # - No duplicates
    count_duplicates_sql = \
            "select count(*) \
                from {0}.{1} \
                group by citing, data \
                having count(*) > 1;"
    sql_command = count_duplicates_sql.format(schema_name, expanded_table_name)
    logger.debug("Executing SQL: %s", sql_command)
    count_duplicates = connection.execute(sql_command).rowcount
    if count_duplicates > 0:
        raise Exception("There are duplicate entries with the same citing, doi, pid and url fields")

def join_tables(connection, inspector, previous_schema_name, expanded_table_name, joint_table):
    joint_table = "joint_"+table_name
    join_tables_sql = \
            "create table {0}.{3} as \
                select \
                    {0}.{2}.id as id, \
                    {0}.{2}.citing as citing, \
                    {0}.{2}.doi as doi, \
                    {0}.{2}.pid as pid, \
                    {0}.{2}.url as url, \
                    {0}.{2}.score as score, \
                    {0}.{2}.source as source, \
                    {1}.{2}.id as previous_id, \
                    {1}.{2}.citing as previous_citing, \
                    {1}.{2}.doi as previous_doi, \
                    {1}.{2}.pid as previous_pid, \
                    {1}.{2}.url as previous_url, \
                    {1}.{2}.score as previous_score, \
                    {1}.{2}.source as previous_source \
                from {1}.{2} full join {0}.{2} \
                on \
                    {0}.{2}.citing={1}.{2}.citing \
                    and {0}.{2}.data={1}.{2}.data;"
    sql_command = join_tables_sql.format(schema_name, previous_schema_name, expanded_table_name, joint_table)
    logger.debug("Executing SQL: %s", sql_command)
    connection.execute(sql_command)

    status_enum_name = schema_name+".status_type"
    enum_names = [e['name'] for e in inspector.get_enums(schema=schema_name)]
    if status_enum_name not in enum_names:
        connection.execute("CREATE TYPE {0} AS ENUM ('NEW', 'DELETED', 'IDENTICAL');".format(status_enum_name))
    add_status_column = "ALTER TABLE {0}.{1} ADD COLUMN status {2};"
    sql_command = add_status_column.format(schema_name,  joint_table, status_enum_name)
    logger.debug("Executing SQL: %s", sql_command)
    connection.execute(sql_command)

def calculate_delta(connection, schema_name, joint_table):
    update_status_identical_sql = \
       "update {0}.{1} \
        set status='IDENTICAL' \
        where \
            {0}.{1}.id is not null \
            and {0}.{1}.previous_id is not null;"

    sql_command = update_status_identical_sql.format(schema_name, joint_table)
    logger.debug("Executing SQL: %s", sql_command)
    connection.execute(sql_command)

    update_status_new_sql = \
       "update {0}.{1} \
        set status='NEW' \
        where \
            {0}.{1}.id is not null \
            and {0}.{1}.previous_id is null;"

    sql_command = update_status_new_sql.format(schema_name, joint_table)
    logger.debug("Executing SQL: %s", sql_command)
    connection.execute(sql_command)

    update_status_deleted_sql = \
       "update {0}.{1} \
        set status='DELETED' \
        where \
            {0}.{1}.id is null \
            and {0}.{1}.previous_id is not null;"

    sql_command = update_status_deleted_sql.format(schema_name, joint_table)
    logger.debug("Executing SQL: %s", sql_command)
    connection.execute(sql_command)


def run(refids, **kwargs):
    """
    Process file specified by the user.

    :param refids: path to the file containing the citations
    :param kwargs: extra keyword arguments
    :return: no return
    """

    logger.info('Loading records from: {0}'.format(refids))

    if 'force' in kwargs:
        force = kwargs['force']
    else:
        force = False

    if 'diagnose' in kwargs:
        diagnose = kwargs['diagnose']
    else:
        diagnose = False

    # Engine
    sqlachemy_url = config.get('SQLALCHEMY_URL', 'postgres://user:password@localhost:5432/citation_capture_pipeline')
    sqlalchemy_echo = config.get('SQLALCHEMY_ECHO', False)
    engine = create_engine(sqlachemy_url, echo=sqlalchemy_echo)
    inspector = Inspector.from_engine(engine)
    connection = engine.connect()

    # Schema for current file
    last_modification_date = datetime.fromtimestamp(os.stat(refids).st_mtime)
    schema_prefix = "testing_"
    schema_name = schema_prefix + last_modification_date.strftime("%Y%m%d_%H%M%S")

    # Table for current file
    RawCitation = build_raw_citation_table(schema=schema_name)
    table_name = RawCitation.__tablename__

    schema_names = inspector.get_schema_names()
    schema_names = filter(lambda x: x.startswith(schema_prefix), schema_names)
    if schema_name not in schema_names:
        connection.execute(CreateSchema(schema_name))

    # Previous schema name if any
    if len(schema_names) > 0:
        schema_names.sort(reverse=True)
        previous_schema_name = schema_names[0]
        for old_schema_name in schema_names[1:]:
            drop_schema = "drop schema {0} cascade;"
            sql_command = drop_schema.format(old_schema_name)
            logger.debug("Executing SQL: %s", sql_command)
            connection.execute(sql_command)
    else:
        previous_schema_name = None

    if table_name not in inspector.get_table_names(schema=schema_name):
        RawCitation.__table__.create(bind=engine)

        # Import a tab-delimited file
        with open(refids) as fp:
            postgres_copy.copy_from(fp, RawCitation, engine, columns=('bibcode', 'payload'))

        expanded_table_name = "expanded_" + table_name

        drop_expanded_table = "drop table if exists {};"
        sql_command = drop_expanded_table.format(expanded_table_name)
        logger.debug("Executing SQL: %s", sql_command)
        connection.execute(sql_command)

        create_expanded_table = \
                "create table {0}.{2} as \
                    select id, \
                        payload->>'citing' as citing, \
                        payload->>'cited' as cited, \
                        payload->>'doi' as doi, \
                        payload->>'pid' as pid, \
                        payload->>'url' as url, \
                        concat(payload->>'doi'::text, payload->>'pid'::text, payload->>'url'::text) as data, \
                        payload->>'score' as score, \
                        payload->>'source' as source \
                    from {0}.{1};"
        sql_command = create_expanded_table.format(schema_name, table_name, expanded_table_name)
        logger.debug("Executing SQL: %s", sql_command)
        connection.execute(sql_command)

        try:
            verify_input_data(connection, schema_name, expanded_table_name)
        except:
            logger.exception("Input data does not comply with some assumptions")
            raise

        if previous_schema_name is not None:
            #joint_table = join_tables(connection, inspector, previous_schema_name, expanded_table_name)
            #calculate_delta(connection, schema_name, joint_table)
            pass
        else:
            # ALL is NEW
            pass

        message = None
        if diagnose:
            print("Calling 'task_check_citation' with '{}'".format(str(message)))
        logger.debug("Calling 'task_check_citation' with '%s'", str(message))
        tasks.task_check_citation.delay(message)


def build_diagnostics(bibcodes=None, json_payloads=None):
    """
    Builds a temporary file to be used for diagnostics.
    """
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    print("Preparing diagnostics temporary file '{}'...".format(tmp_file.name))
    for bibcode, json_payload in zip(bibcodes, json_payloads):
        tmp_str = '{}\t{}'.format(bibcode, json_payload)
        print("\t{}".format(tmp_str))
        tmp_file.write(tmp_str+"\n")
    tmp_file.close()
    return tmp_file.name

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Process user input.')

    parser.add_argument('-r',
                        '--refids',
                        dest='refids',
                        action='store',
                        type=str,
                        help='Path to the refids.dat file'
                             ' that contains the citation list.')

    parser.add_argument('-f',
                        '--force',
                        dest='force',
                        action='store_true',
                        help='Force the processing of all the citations')

    parser.add_argument('-d',
                        '--diagnose',
                        dest='diagnose',
                        action='store_true',
                        default=False,
                        help='Process specific diagnostic default data')

    parser.add_argument('-b',
                        '--bibcodes',
                        dest='bibcodes',
                        action='store',
                        default=None,
                        help='Comma delimited list of bibcodes (for diagnostics)')

    parser.add_argument('-j',
                        '--json',
                        dest='json',
                        action='store',
                        default=None,
                        help='Semicolon delimited list of json citation (for diagnostics)')

    parser.set_defaults(refids=False)
    parser.set_defaults(force=False)
    parser.set_defaults(diagnose=False)

    args = parser.parse_args()

    if args.diagnose:
        if args.bibcodes:
            args.bibcodes = [x.strip() for x in args.bibcodes.split(',')]
        else:
            # Defaults
            args.bibcodes = ["1005PhRvC..71c4906H", "1915PA.....23..189P", "2017PASP..129b4005R"]

        if args.json:
            args.json = [x.strip() for x in args.json.split(';')]
        else:
            # Defaults
            args.json = [
                    "{\"cited\":\"1976NuPhB.113..395J\",\"citing\":\"1005PhRvC..71c4906H\",\"doi\":\"10.1016/0550-3213(76)90133-4\",\"score\":\"1\",\"source\":\"/proj/ads/references/resolved/PhRvC/0071/1005PhRvC..71c4906H.ref.xml.result:17\"}",
                    "{\"cited\":\"...................\",\"citing\":\"2017SSEle.128..141M\",\"score\":\"0\",\"source\":\"/proj/ads/references/resolved/SSEle/0128/10.1016_j.sse.2016.10.029.xref.xml.result:10\",\"url\":\"https://github.com/viennats/viennats-dev\"}",
                    "{\"cited\":\"2013ascl.soft03021B\",\"citing\":\"2017PASP..129b4005R\",\"pid\":\"ascl:1303.021\",\"score\":\"1\",\"source\":\"/proj/ads/references/resolved/PASP/0129/iss972.iop.xml.result:114\"}",
                    ]

        args.refids = build_diagnostics(json_payloads=args.json, bibcodes=args.bibcodes)

    if not args.refids:
        print 'You need to give the input list'
        parser.print_help()
        sys.exit(0)

    # Send the files to be put on the queue
    run(args.refids,
        force=args.force,
        diagnose=args.diagnose)

    if args.diagnose:
        print("Removing diagnostics temporary file '{}'".format(args.refids))
        os.unlink(args.refids)
