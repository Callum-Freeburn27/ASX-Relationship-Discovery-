import logging

import pandas as pd
import sshtunnel
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder

global tunnel

ssh_host = '192.168.56.1'       # put your own ip address here
ssh_username = 'disco_stock_data_rels'
ssh_password = '5skE64j&Q'
database_username = 'admin'
database_password = '5skE64j&Q'
database_name = 'stockmarketdata'
localhost = '127.0.0.1'


def open_ssh_tunnel(verbose=False):
    """Open an SSH tunnel and connect using a username and password.
            """
    if verbose:
        sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG

    tunnel = SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username=ssh_username,
        ssh_password=ssh_password,
        remote_bind_address=('127.0.0.1', 3306)
    )
    tunnel.start()
    return tunnel


def mysql_connect():
    """Connect to a MySQL server using the SSH tunnel connection
    :return connection: Global MySQL database connection
    """
    tunnel = open_ssh_tunnel()
    local_port = str(tunnel.local_bind_port)
    engine = create_engine('mysql+pymysql://admin:5skE64j&Q@127.0.0.1:' + local_port + '/stockmarketdata')

    return engine


# def write_query():
#     """Runs a given SQL query via the global database connection.
#
#         :param sql: MySQL query
#         :return: Pandas dataframe containing results
#         """
#
#     return pd.read_sql_query(sql, connection)
#
#
# def mysql_disconnect():
#     """Closes the MySQL database connection.
#         """
#
#     connection.close()


def close_ssh_tunnel():
    """Closes the SSH tunnel connection.
        """
    tunnel.close()
