#!/usr/bin/env python
import os
import sys
import tempfile
import argparse
import json
from astropy.io import ascii
from ADSCitationCapture import tasks, db
from ADSCitationCapture.delta_computation import DeltaComputation

# ============================= INITIALIZATION ==================================== #

from adsputils import setup_logging, load_config
proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)
logger = setup_logging('run.py', proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'INFO'),
                        attach_stdout=config.get('LOG_STDOUT', False))

# =============================== FUNCTIONS ======================================= #


def process(refids_filename, **kwargs):
    """
    Process file specified by the user.

    :param refids_filename: path to the file containing the citations
    :param kwargs: extra keyword arguments
    :return: no return
    """

    logger.info('Loading records from: %s', refids_filename)

    force = kwargs.get('force', False)
    diagnose = kwargs.get('diagnose', False)
    if diagnose:
        schema_prefix = "diagnose_citation_capture_"
    else:
        schema_prefix = kwargs.get('schema_prefix', "citation_capture_")

    # Engine
    sqlachemy_url = kwargs.get('sqlalchemy_url', config.get('SQLALCHEMY_URL', 'postgres://user:password@localhost:5432/citation_capture_pipeline'))
    sqlalchemy_echo = config.get('SQLALCHEMY_ECHO', False)

    delta = DeltaComputation(sqlachemy_url, sqlalchemy_echo=sqlalchemy_echo, group_changes_in_chunks_of=1, schema_prefix=schema_prefix, force=force)
    delta.compute(refids_filename)
    for changes in delta:
        if diagnose:
            print("Calling 'task_process_citation_changes' with '{}'".format(str(changes)))
        logger.debug("Calling 'task_process_citation_changes' with '%s'", str(changes))
        try:
            tasks.task_process_citation_changes.delay(changes, force=force)
        except:
            # In asynchronous mode, no exception is expected
            # In synchronous mode (for debugging purposes), exception may happen (e.g., failures to fetch metadata)
            logger.exception('Exception produced while processing citation changes')
    if diagnose:
        delta._execute_sql("drop schema {0} cascade;", delta.schema_name)
    delta.connection.close()

def maintenance_canonical(dois, bibcodes):
    """
    Updates canonical bibcodes (e.g., arXiv bibcodes that were merged with publisher bibcodes)
    Records that do not have the status 'REGISTERED' in the database will not be updated
    """
    n_requested = len(dois) + len(bibcodes)
    if n_requested == 0:
        logger.info("MAINTENANCE task: requested an update of all the canonical bibcodes")
    else:
        logger.info("MAINTENANCE task: requested an update of '{}' canonical bibcodes".format(n_requested))

    # Send to master updated citation bibcodes in their canonical form
    tasks.task_maintenance_canonical.delay(dois, bibcodes)

def maintenance_metadata(dois, bibcodes):
    """
    Refetch metadata and send updates to master (if any)
    """
    n_requested = len(dois) + len(bibcodes)
    if n_requested == 0:
        logger.info("MAINTENANCE task: requested a metadata update for all the registered records")
    else:
        logger.info("MAINTENANCE task: requested a metadata update for '{}' records".format(n_requested))

    # Send to master updated metadata
    tasks.task_maintenance_metadata.delay(dois, bibcodes)

def maintenance_resend(dois, bibcodes, broker=False):
    """
    Re-send records to master
    """
    n_requested = len(dois) + len(bibcodes)
    if n_requested == 0:
        logger.info("MAINTENANCE task: re-sending all the registered records")
    else:
        logger.info("MAINTENANCE task: re-sending '{}' records".format(n_requested))

    # Send to master updated metadata
    tasks.task_maintenance_resend.delay(dois, bibcodes, broker)

def maintenance_reevaluate(dois, bibcodes):
    """
    Re-send records to master
    """
    n_requested = len(dois) + len(bibcodes)
    if n_requested == 0:
        logger.info("MAINTENANCE task: re-sending all the registered records")
    else:
        logger.info("MAINTENANCE task: re-sending '{}' records".format(n_requested))

    # Send to master updated metadata
    tasks.task_maintenance_reevaluate.delay(dois, bibcodes)

def maintenance_curation(filename = None, dois = None, bibcodes = None, json = None, reset = False, show = False):
    """
    Refetch metadata and update any manually curated values.
    """
    #checks if file is specificed
    if filename is not None:
        with open(filename) as f:
            try:
                #convert file lines to list of dicts, 1 dict per entry.
                curated_entries = [json.loads(i) for i in f.read().splitlines()]

            except Exception as e:
                msg = "Parsing file: {}, failed with Exception: {}. Please check each entry is properly formatted.".format(filename, e)
                logger.error(msg)
                raise

            #collect bibcodes from entries if available.
            bibcodes = list(filter(lambda entry:(entry.get('bibcode', None) is not None), curated_entries))
            #collect dois if no bibcode is available.
            dois = list(filter(lambda entry:(entry.get('doi', None) is not None and entry.get('bibcode', None) is None), curated_entries))
        
            n_requested = len(dois) + len(bibcodes)

            logger.info("MAINTENANCE task: requested a metadata update for '{}' records".format(n_requested))

        # Update metadata and forward to master
        if n_requested != 0:
            tasks.task_maintenance_curation.delay(dois, bibcodes, curated_entries, reset)
        else:
            logger.info("No targets specified for curation.")

    elif dois is not None or bibcodes is not None:
        if reset:
            n_requested = len(dois) + len(bibcodes)
            logger.info("MAINTENANCE task: requested deletion of curated metadata for '{}' records.".format(n_requested))
            curated_entries =[{"bibcode":bib} for bib in bibcodes]+[{"doi":doi} for doi in dois]
            tasks.task_maintenance_curation.delay(dois, bibcodes, curated_entries, reset)
        elif show:
            n_requested = len(dois) + len(bibcodes)
            curated_entries =[{"bibcode":bib} for bib in bibcodes]+[{"doi":doi} for doi in dois]
            print(curated_entries)
            logger.info("MAINTENANCE task: Displaying current metadata for '{}' record(s).".format(n_requested))
            tasks.task_maintenance_show_metadata.delay(curated_entries)
        elif json:
            try:
                #convert json line to list of dicts, 1 dict per entry.
                curated_entries = [json.loads(json)]
                if dois:
                    curated_entries[0]['doi'] = dois[0]
                elif bibcodes:
                    curated_entries[0]['bibcode'] = bibcodes[0]
            except Exception as e:
                msg = "Parsing json arg: {}, failed with Exception: {}. Please check each entry is properly formatted.".format(filename, e)
                logger.error(msg)
                raise
            
            n_requested = len(dois) + len(bibcodes)

            logger.info("MAINTENANCE task: requested a metadata update for '{}' records".format(n_requested))
            tasks.task_maintenance_curation.delay(dois, bibcodes, curated_entries, reset)

    else:
        logger.error("MAINTENANCE task: manual curation failed. Please specify a file containing the modified citations.")


def diagnose(bibcodes, json):
    citation_count = db.get_citation_count(tasks.app)
    citation_target_count = db.get_citation_target_count(tasks.app)
    if citation_count != 0 or citation_target_count != 0:
        logger.error("Diagnose aborted because the database already contains %s citations and %s citations targets (this is a protection against modifying a database in use)", citation_count, citation_target_count)
    else:
        if not bibcodes:
            bibcodes = ["1005PhRvC..71c4906H", "1915PA.....23..189P", "2017PASP..129b4005R"]
            logger.info('Using default bibcodes for diagnose:\n\t%s', "\n\t".join(bibcodes))

        if not json:
            json = [
                    "{\"cited\":\"1976NuPhB.113..395J\",\"citing\":\"1005PhRvC..71c4906H\",\"doi\":\"10.1016/0550-3213(76)90133-4\",\"score\":\"1\",\"source\":\"/proj/ads/references/resolved/PhRvC/0071/1005PhRvC..71c4906H.ref.xml.result:17\"}",
                    "{\"cited\":\"...................\",\"citing\":\"2017SSEle.128..141M\",\"score\":\"0\",\"source\":\"/proj/ads/references/resolved/SSEle/0128/10.1016_j.sse.2016.10.029.xref.xml.result:10\",\"url\":\"https://github.com/viennats/viennats-dev\"}",
                    "{\"cited\":\"2013ascl.soft03021B\",\"citing\":\"2017PASP..129b4005R\",\"pid\":\"ascl:1303.021\",\"score\":\"1\",\"source\":\"/proj/ads/references/resolved/PASP/0129/iss972.iop.xml.result:114\"}",
                    ]
            logger.info('Using default json data for diagnose:\n\t%s', "\n\t".join(json))

        input_filename = _build_diagnostics(json_payloads=json, bibcodes=bibcodes)

        # Process diagnostic data
        process(input_filename, force=False, diagnose=True)



def _build_diagnostics(bibcodes=None, json_payloads=None):
    """
    Builds a temporary file to be used for diagnostics.
    """
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    print("Preparing diagnostics temporary file '{}'...".format(tmp_file.name))
    for bibcode, json_payload in zip(bibcodes, json_payloads):
        tmp_str = '{}\t{}'.format(bibcode, json_payload)
        print("\t{}".format(tmp_str))
        tmp_file.write((tmp_str+"\n").encode('UTF-8'))
    tmp_file.close()
    os.utime(tmp_file.name, (0, 0)) # set the access and modified times to 19700101_000000
    return tmp_file.name

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='commands', dest="action")
    process_parser = subparsers.add_parser('PROCESS', help='Process input file, compare to previous data in database, and execute insertion/deletions/updates of citations')
    process_parser.add_argument('input_filename',
                        action='store',
                        type=str,
                        help='Path to the input file (e.g., refids.dat) file that contains the citation list')
    maintenance_parser = subparsers.add_parser('MAINTENANCE', help='Execute maintenance task')
    maintenance_parser.add_argument(
                        '--resend',
                        dest='resend',
                        action='store_true',
                        default=False,
                        help='Re-send registered citations and targets to the master pipeline')
    maintenance_parser.add_argument(
                        '--curation',
                        dest='curation',
                        action='store_true',
                        default=False,
                        help='Manually edit Citation Capture generated citation metadata for specific entries.')
    maintenance_parser.add_argument(
                        '--resend-broker',
                        dest='resend_broker',
                        action='store_true',
                        default=False,
                        help='Re-send registered citations and targets to the broker')
    maintenance_parser.add_argument(
                        '--reevaluate',
                        dest='reevaluate',
                        action='store_true',
                        default=False,
                        help='Re-evaluate discarded citation targets fetching metadata and ingesting software records')
    maintenance_parser.add_argument(
                        '--canonical',
                        dest='canonical',
                        action='store_true',
                        default=False,
                        help='Update citations with canonical bibcodes')
    maintenance_parser.add_argument(
                        '--metadata',
                        dest='metadata',
                        action='store_true',
                        default=False,
                        help='Update DOI metadata for the provided list of citation target bibcodes, or if none is provided, for all the current existing citation targets')
    maintenance_parser.add_argument(
                        '--doi',
                        dest='dois',
                        nargs='+',
                        action='store',
                        default=[],
                        help='Space separated DOI list (e.g., 10.5281/zenodo.10598), if no list is provided then the full database is considered')
    maintenance_parser.add_argument(
                        '--bibcode',
                        dest='bibcodes',
                        nargs='+',
                        action='store',
                        default=[],
                        help='Space separated bibcode list, if no list is provided then the full database is considered')
    maintenance_parser.add_argument('--input_filename',
                        action='store',
                        type=str,
                        help='Path to the input file (e.g., refids.dat) file that contains the citation list')
    maintenance_parser.add_argument('--reset',
                        action='store_true',
                        default=False,
                        help='Delete manually curated metadata for supplied bibcodes.')
    maintenance_parser.add_argument('--show',
                        action='store_true',
                        default=False,
                        help='Show current metadata for a given citation target.')
    diagnose_parser = subparsers.add_parser('DIAGNOSE', help='Process data for diagnosing infrastructure')
    diagnose_parser.add_argument(
                        '--bibcodes',
                        dest='bibcodes',
                        nargs='+',
                        action='store',
                        default=None,
                        help='Space delimited list of bibcodes')
    diagnose_parser.add_argument(
                        '--json',
                        dest='json',
                        nargs='+',
                        action='store',
                        default=None,
                        help='Space delimited list of json citation data')

    args = parser.parse_args()

    if args.action == "PROCESS":
        if not os.path.exists(args.input_filename):
            process_parser.error("the file '{}' does not exist".format(args.input_filename))
        elif not os.access(args.input_filename, os.R_OK):
            process_parser.error("the file '{}' cannot be accessed".format(args.input_filename))
        else:
            logger.info("PROCESS task: %s", args.input_filename)
            process(args.input_filename, force=False, diagnose=False)

    elif args.action == "MAINTENANCE":
        if not args.canonical and not args.metadata and not args.resend and not args.resend_broker and not args.reevaluate and not args.curation:
            maintenance_parser.error("nothing to be done since no task has been selected")
        else:
            # Read files if provided (instead of a direct list of DOIs)
            if len(args.dois) == 1 and os.path.exists(args.dois[0]):
                logger.info("Reading DOIs from file '%s'", args.dois[0])
                table = ascii.read(args.dois[0], delimiter="\t", names=('doi', 'version'))
                dois = table['doi'].tolist()
            else:
                dois = args.dois
            # Read files if provided (instead of a direct list of bibcodes)
            if len(args.bibcodes) == 1 and os.path.exists(args.bibcodes[0]):
                logger.info("Reading bibcodes from file '%s'", args.bibcodes[0])
                table = ascii.read(args.bibcodes[0], delimiter="\t", names=('bibcode', 'version'))
                bibcodes = table['bibcode'].tolist()
            else:
                bibcodes = args.bibcodes
            # Process
            if args.metadata:
                maintenance_metadata(dois, bibcodes)
            elif args.canonical:
                maintenance_canonical(dois, bibcodes)
            elif args.resend:
                maintenance_resend(dois, bibcodes, broker=False)
            elif args.resend_broker:
                maintenance_resend(dois, bibcodes, broker=True)
            elif args.reevaluate:
                maintenance_reevaluate(dois, bibcodes)
            elif args.curation:
                maintenance_curation(args.input_filename, dois, bibcodes, json, args.reset, args.show)

    elif args.action == "DIAGNOSE":
        logger.info("DIAGNOSE task")
        diagnose(args.bibcodes, args.json)
    else:
        raise Exception("Unknown argument action: {}".format(args.action))

