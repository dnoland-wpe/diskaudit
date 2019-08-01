#!/usr/bin/env python3

# Import libraries
from __future__ import print_function
import os
import re
import sys
import socket
import psutil
import subprocess

from wpepy.wpe_api import WpeApi
from wpepy.wpe_api import WpeApiV2

"""
Color variables
"""
# Color effects
NC = '\033[0m'
bold = '\033[01m'
disable = '\033[02m'
underline = '\033[04m'
reverse = '\033[07m'
strikethrough = '\033[09m'
invisible = '\033[08m'

# Foreground colors:
green = '\033[32m'
orange = '\033[33m'
ltred = '\033[91m'
ltgreen = '\033[92m'
yellow = '\033[93m'

"""
Initial sanity checks
"""
# Check available server memory resources
# If current memory usage is less that 1GB, exit program
# Otherwise, proceed with additional sanity checks
memory = psutil.virtual_memory()
if (int(memory.free) < 268435456):
    sys.exit('Server memory too low to execute.')
else:
    print(ltgreen,'Server resources look good. Proceeding with audit...', NC)

# Check for install directory
if not re.match('/nas/content/(live|staging)/\w+', os.getcwd()):
    sys.exit('Not in a site dir? CD to an install and retry.')

# Non-root check
if os.environ.get('USERNAME') == 'root':
    sys.exit('Diskaudit works without running as root!', NC,
             'Please try as your regular user.')

"""
Utility functions
"""


def fix_format(value):
    # Format raw diskspace and data counts from bytes to KB, MB, or GB
    if value < 1024:
        unit = "B"
        change = value
    elif value >= 1024 and value < 1048576:
        unit = "K"
        change = '{0:.2f}'.format(value / 1024)
    elif value >= 1048576 and value < 1073741824:
        unit = "M"
        change = '{0:.2f}'.format(value / 1024 / 1024)
    elif value >= 1073741824 and value < 1099511627776:
        unit = "G"
        change = '{0:.2f}'.format(value / 1024 / 1024 / 1024)
    else:
        unit = "T"
        change = '{0:.2f}'.format(value / 1024 / 1024 / 1024 / 1024)
    value = '{}{}'.format(change, unit)
    return value


def run_cli_query(install, query, data_only=True):
    """Run db query using wp-cli
    :param query: query to run
    :param data_only: bool, exclude column names
    :return: string of query results
    """
    cmd = ['wp', '--path=/nas/content/live/{}'.format(install),
           'db', 'query', query]
    if data_only:
        cmd.append('--skip-column-names')
        return subprocess.check_output(cmd).strip()


def run_query(query, data_only=True):
    """Run db query using wp-cli
    :param query: query to run
    :param data_only: bool, exclude column names
    :return: string of query results
    """
    cmd = ['wp', 'db', 'query', query]
    if data_only:
        cmd.append('--skip-column-names')
        return subprocess.check_output(cmd).strip()


def get_vendor(host):
    return sm_data['provider'].decode('ascii')


def get_host():
    with open("/etc/cluster-id", "r") as cid:
        return cid.read().split()[0]


def check_multisite(install, is_multisite="No"):
    ck_mu = ['php', '/nas/wp/www/tools/wpe.php', 'option-get', install, 'mu']
    mu = subprocess.check_output(ck_mu)
    if mu == '1':
        is_multisite = "Yes"
    return is_multisite


def get_innodb_buffer_size():
    ibp = int(run_query('SELECT @@GLOBAL.innodb_buffer_pool_size;'))
    return fix_format(ibp)


"""
Auditing functions
"""


def get_prod_du(install):
    # Calculate production filesystem diskusage
    if not os.path.exists('/nas/content/live/'+install):
        prod_du = 0
        return prod_du
    else:
        try:
            prod_du = float(subprocess.check_output(
                ['du', '-sb',
                 '/nas/content/live/'+install]).split()[0]
            )
        except (IOError):
            prod_du = 0
    return prod_du


def get_prod_db_du(install):
    # Calculate production database diskusage
    if vendor == "google":
        prod_db_du = float(subprocess.check_output(
            ['sudo', 'du', '-sb', '/nas/mysql/wp_'+install]).split()[0])
    else:
        prod_db_du = 0
    return prod_db_du


def get_prod_db_data(install):
    # Calculate production database data amount
    db_name = "wp_{}".format(install)
    prod_db = float(run_cli_query(install,
                    "USE information_schema;\
                     SELECT SUM(data_length + index_length)\
                        FROM information_schema.TABLES\
                    WHERE\
                        table_schema = '{db}' and\
                        TABLE_TYPE='BASE TABLE';".format(db=db_name)))
    return prod_db


def get_stage_du(install):
    # Calculate staging diskusage
    if not os.path.exists('/nas/content/staging/{}'.format(install)):
        stage_du = 0
        return stage_du
    else:
        try:
            stage_du = float(subprocess.check_output(
                ['du', '-sb',
                 '/nas/content/staging/{}'.format(install)]).split()[0]
            )
        except (IOError):
            stage_du = 0
    if stage_du == 4096:
        stage_du = 0
    return stage_du


def get_stage_db_data(install):
    # Calculate staging database data amount
    db_name = "snapshot_{}".format(install)
    stage_db = run_cli_query(install,
                             "USE information_schema;\
                              SELECT SUM(data_length + index_length)\
                                  FROM information_schema.TABLES\
                              WHERE\
                                  table_schema = '{db}' and\
                                  TABLE_TYPE='BASE TABLE';".format(db=db_name))
    if stage_db == 'NULL':
        stage_db = 0
    return int(stage_db)


def create_install_stats_dictionary(install_list):
    install_stats_dict = {}
    for install in install_list:
        install_stats_dict[install] = {
            "Multisite": check_multisite(install),
            "Production": get_prod_du(install),
            "wp_disk": get_prod_db_du(install),
            "wp_data": get_prod_db_data(install),
            "Staging": get_stage_du(install),
            "ss_data": get_stage_db_data(install)}
    return install_stats_dict


"""
Main body of program
"""
host = int(get_host())
wapi = WpeApi()
sm_data = wapi.get.cluster_info(host)
# Check pod number and ensure is a Google pod value
vendor = get_vendor(host)
if len(sys.argv[1:]) == 0:  # Execute if no installs provided in command line
    wapi2 = WpeApiV2()
    # Get a list of ACTIVE installs on server/cluster and assign to list
    install_list = map(lambda x: x.encode('ascii'),
                       wapi2.sites_on_cluster(host))
else:
    install_count = len(sys.argv[1:])  # Execute for only installs provided
    install_list = sys.argv[1:]
if vendor == "amazon":  # Execute if on an Amazon cluster
    hostname = socket.gethostname()
    print('Conducting', ltred, 'diskaudit', NC, '...')
    print('Executing calculations,', orange,
          'this could take a few minutes', NC, '.')
    # refresh summary total variables
    sum_dictionary = {
        "prod_du_ttl": 0,
        "prod_db_du_ttl": 0,
        "prod_db_ttl": 0,
        "stage_du_ttl": 0,
        "stage_db_ttl": 0
    }
    install_stats = create_install_stats_dictionary(install_list)
    print(ltgreen, '\n Pod:', NC, host)
    print(ltgreen, 'InnoDB Buffer Pool Size:', NC,
          get_innodb_buffer_size(), '\n')
    # table formatting for column uniformity
    header = '{:<18}{:>10}{:>13}{:>12}{:>11}{:>11}'
    body = '{}{}{:<16}{}{:>12}{:>13}{:>12}{:>11}{:>11}'
    footer = '{}{:>26}{:>12}{:>12}{:>11}{:>11}'
    print(green, header.format(
        'Install',
        'Multisite',
        'Production',
        'Prod DB',
        'Staging',
        'Stage DB'), NC)
    print(green, header.format(
        '-------',
        '---------',
        '----------',
        '--------',
        '-------',
        '---------', NC))
    for install in install_list:
        print(body.format(
              orange,
              ' ',
              install, NC,
              install_stats[install]['Multisite'],
              fix_format(install_stats[install]['Production']),
              fix_format(install_stats[install]['wp_data']),
              fix_format(install_stats[install]['Staging']),
              fix_format(install_stats[install]['ss_data'])))
        # increment summary totals
        sum_dictionary['prod_du_ttl'] += install_stats[install]['Production']
        sum_dictionary['prod_db_ttl'] += install_stats[install]['wp_data']
        sum_dictionary['stage_du_ttl'] += install_stats[install]['Staging']
        sum_dictionary['stage_db_ttl'] += install_stats[install]['ss_data']
    print(green, header.format(
          '-------',
          '---------',
          '----------',
          '--------',
          '-------',
          '---------', NC))
    print(green, footer.format(
          'Totals:', NC,
          fix_format(sum_dictionary['prod_du_ttl']),
          fix_format(sum_dictionary['prod_db_ttl']),
          fix_format(sum_dictionary['stage_du_ttl']),
          fix_format(sum_dictionary['stage_db_ttl'])))
    print(ltgreen, 'Total diskusage:', NC,
          fix_format(sum_dictionary['prod_du_ttl'] +
                     sum_dictionary['stage_du_ttl']))
    print(ltgreen, 'Combined DB Size:', NC,
          fix_format(sum_dictionary['prod_db_ttl'] +
                     sum_dictionary['stage_db_ttl']))
else:
    hostname = socket.gethostname()
    print('Conducting', ltred, 'diskaudit', NC, '...')
    print('Executing calculations,', orange,
          'this could take a few minutes', NC, '.')
    # refresh summary total variables
    sum_dictionary = {
        "prod_du_ttl": 0,
        "prod_db_du_ttl": 0,
        "prod_db_ttl": 0,
        "stage_du_ttl": 0,
        "stage_db_ttl": 0}
    install_stats = create_install_stats_dictionary(install_list)
    print(ltgreen, '\n Pod:', NC, host)
    print(ltgreen, 'InnoDB Buffer Pool Size:', NC,
          get_innodb_buffer_size(), '\n')
    # table formatting for column uniformity
    header = '{:<18}{:>10}{:>13}{:>13}{:>12}{:>11}{:>13}'
    body = '{}{}{:<14}{}{:>14}{:>13}{:>13}{:>12}{:>11}{:>13}'
    footer = '{}{:>26}{:>12}{:>13}{:>11}{:>12}{:>13}'
    print(green, header.format(
          'Install',
          'Multisite',
          'Production',
          'Prod DB DU',
          'Prod DB',
          'Staging',
          'Staging DB'), NC)
    print(green, header.format(
          '-------',
          '---------',
          '----------',
          '----------',
          '--------',
          '-------',
          '----------',
          '----------'), NC)
    for install in install_list:
        print(body.format(
            orange,
            ' ',
            install, NC,
            install_stats[install]['Multisite'],
            fix_format(install_stats[install]['Production']),
            fix_format(install_stats[install]['wp_disk']),
            fix_format(install_stats[install]['wp_data']),
            fix_format(install_stats[install]['Staging']),
            fix_format(install_stats[install]['ss_data'])))
        # increment summary totals
        sum_dictionary['prod_du_ttl'] += install_stats[install]['Production']
        sum_dictionary['prod_db_du_ttl'] += install_stats[install]['wp_disk']
        sum_dictionary['prod_db_ttl'] += install_stats[install]['wp_data']
        sum_dictionary['stage_du_ttl'] += install_stats[install]['Staging']
        sum_dictionary['stage_db_ttl'] += install_stats[install]['ss_data']
    print(green, header.format(
        '-------',
        '---------',
        '----------',
        '---------',
        '--------',
        '-------',
        '----------',
        '---------'), NC)
    print(green, footer.format(
        'Totals:', NC,
        fix_format(sum_dictionary['prod_du_ttl']),
        fix_format(sum_dictionary['prod_db_du_ttl']),
        fix_format(sum_dictionary['prod_db_ttl']),
        fix_format(sum_dictionary['stage_du_ttl']),
        fix_format(sum_dictionary['stage_db_ttl'])))
    print(ltgreen, '\n Total diskusage:', NC,
          fix_format(sum_dictionary['prod_du_ttl'] +
                     sum_dictionary['prod_db_du_ttl'] +
                     sum_dictionary['stage_du_ttl']))
    print(ltgreen, 'Combined DB Size:', NC,
          fix_format(sum_dictionary['prod_db_ttl'] +
                     sum_dictionary['stage_db_ttl']))
