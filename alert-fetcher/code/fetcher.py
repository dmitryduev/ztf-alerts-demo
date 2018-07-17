import argparse
import pymongo
import os
import inspect
import json
import logging
import datetime
import time
import shutil
import traceback


class Fetcher(object):

    def __init__(self, _config_file):
        """

        :param _config_file:
        """
        ''' load config data '''
        self.config = self.get_config(_config_file)

        ''' set up logging at init '''
        self.logger, self.logger_utc_date = self.set_up_logging(_name='fetcher', _mode='a')

        # make dirs if necessary:
        for _pp in ('alerts', 'tmp'):
            _path = self.config['path']['path_{:s}'.format(_pp)]
            if not os.path.exists(_path):
                os.makedirs(_path)
                self.logger.debug('Created {:s}'.format(_path))

        ''' init db if necessary '''
        self.init_db()

        ''' connect to db: '''
        self.db = None
        self.connect_to_db()

    @staticmethod
    def get_config(_config_file):
        """
            Load config JSON file
        """
        ''' script absolute location '''
        abs_path = os.path.dirname(inspect.getfile(inspect.currentframe()))

        if _config_file[0] not in ('/', '~'):
            if os.path.isfile(os.path.join(abs_path, _config_file)):
                config_path = os.path.join(abs_path, _config_file)
            else:
                raise IOError('Failed to find config file')
        else:
            if os.path.isfile(_config_file):
                config_path = _config_file
            else:
                raise IOError('Failed to find config file')

        with open(config_path) as cjson:
            config_data = json.load(cjson)
            # config must not be empty:
            if len(config_data) > 0:
                return config_data
            else:
                raise Exception('Failed to load config file')

    def set_up_logging(self, _name='fetcher', _mode='w'):
        """ Set up logging
            :param _name:
            :param _mode: overwrite log-file or append: w or a
            :return: logger instance
            """
        # 'debug', 'info', 'warning', 'error', or 'critical'
        if self.config['misc']['logging_level'] == 'debug':
            _level = logging.DEBUG
        elif self.config['misc']['logging_level'] == 'info':
            _level = logging.INFO
        elif self.config['misc']['logging_level'] == 'warning':
            _level = logging.WARNING
        elif self.config['misc']['logging_level'] == 'error':
            _level = logging.ERROR
        elif self.config['misc']['logging_level'] == 'critical':
            _level = logging.CRITICAL
        else:
            raise ValueError('Config file error: logging level must be ' +
                             '\'debug\', \'info\', \'warning\', \'error\', or \'critical\'')

        # get path to logs from config:
        _path = self.config['path']['path_logs']

        if not os.path.exists(_path):
            os.makedirs(_path)
        utc_now = datetime.datetime.utcnow()

        # http://www.blog.pythonlibrary.org/2012/08/02/python-101-an-intro-to-logging/
        _logger = logging.getLogger(_name)

        _logger.setLevel(_level)
        # create the logging file handler
        fh = logging.FileHandler(os.path.join(_path, '{:s}.{:s}.log'.format(_name, utc_now.strftime('%Y%m%d'))),
                                 mode=_mode)
        logging.Formatter.converter = time.gmtime

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # formatter = logging.Formatter('%(asctime)s %(message)s')
        fh.setFormatter(formatter)

        # add handler to logger object
        _logger.addHandler(fh)

        return _logger, utc_now.strftime('%Y%m%d')

    def shut_down_logger(self):
        """
            Prevent writing to multiple log-files after 'manual rollover'
        :return:
        """
        handlers = self.logger.handlers[:]
        for handler in handlers:
            handler.close()
            self.logger.removeHandler(handler)

    def check_logging(self):
        """
            Check if a new log file needs to be started and start it if necessary
        """
        if datetime.datetime.utcnow().strftime('%Y%m%d') != self.logger_utc_date:
            # reset
            self.shut_down_logger()
            self.logger, self.logger_utc_date = self.set_up_logging(_name='archive', _mode='a')

    def init_db(self):
        """
            Initialize db if new Mongo instance
        :return:
        """
        _client = pymongo.MongoClient(username=self.config['database']['admin'],
                                      password=self.config['database']['admin_pwd'],
                                      host=self.config['database']['host'],
                                      port=self.config['database']['port'])
        # _id: db_name.user_name
        user_ids = [_u['_id'] for _u in _client.admin.system.users.find({}, {'_id': 1})]

        db_name = self.config['database']['db']
        username = self.config['database']['user']

        # print(f'{db_name}.{username}')
        # print(user_ids)

        if f'{db_name}.{username}' not in user_ids:
            _client[db_name].command('createUser', self.config['database']['user'],
                                     pwd=self.config['database']['pwd'], roles=['readWrite'])
            print('Successfully initialized db')

    def connect_to_db(self):
        """
            Connect to Robo-AO's MongoDB-powered database
        :return:
        """
        _config = self.config
        try:
            if self.logger is not None:
                self.logger.debug('Connecting to the database at {:s}:{:d}'.
                                  format(_config['database']['host'], _config['database']['port']))
            _client = pymongo.MongoClient(host=_config['database']['host'], port=_config['database']['port'])
            # grab main database:
            _db = _client[_config['database']['db']]

        except Exception as _e:
            if self.logger is not None:
                self.logger.error(_e)
                self.logger.error('Failed to connect to the database at {:s}:{:d}'.
                                  format(_config['database']['host'], _config['database']['port']))
            # raise error
            raise ConnectionRefusedError
        try:
            # authenticate
            _db.authenticate(_config['database']['user'], _config['database']['pwd'])
            if self.logger is not None:
                self.logger.debug('Successfully authenticated with the database at {:s}:{:d}'.
                                  format(_config['database']['host'], _config['database']['port']))
        except Exception as _e:
            if self.logger is not None:
                self.logger.error(_e)
                self.logger.error('Authentication failed for the database at {:s}:{:d}'.
                                  format(_config['database']['host'], _config['database']['port']))
            raise ConnectionRefusedError

        if self.logger is not None:
            self.logger.debug('Successfully connected to the database at {:s}:{:d}'.
                              format(_config['database']['host'], _config['database']['port']))

        # (re)define self.db
        self.db = dict()
        self.db['client'] = _client
        self.db['db'] = _db

    def disconnect_from_db(self):
        """
            Disconnect from Robo-AO's MongoDB database.
        :return:
        """
        self.logger.debug('Disconnecting from the database.')
        if self.db is not None:
            try:
                self.db['client'].close()
                self.logger.debug('Successfully disconnected from the database.')
            except Exception as e:
                self.logger.error('Failed to disconnect from the database.')
                self.logger.error(e)
            finally:
                # reset
                self.db = None
        else:
            self.logger.debug('No connection found.')

    def check_db_connection(self):
        """
            Check if DB connection is alive/established.
        :return: True if connection is OK
        """
        self.logger.debug('Checking database connection.')
        if self.db is None:
            try:
                self.connect_to_db()
            except Exception as e:
                print('Lost database connection.')
                self.logger.error('Lost database connection.')
                self.logger.error(e)
                return False
        else:
            try:
                # force connection on a request as the connect=True parameter of MongoClient seems
                # to be useless here
                self.db['client'].server_info()
            except pymongo.errors.ServerSelectionTimeoutError as e:
                print('Lost database connection.')
                self.logger.error('Lost database connection.')
                self.logger.error(e)
                return False

        return True

    def fetch(self, **kwargs):
        """

        """
        raise NotImplementedError


class FetcherArchive(Fetcher):
    """

    """
    def __init__(self, _config_file):
        """

        :param _config_file:
        """

        ''' initialize super class '''
        super(FetcherArchive, self).__init__(_config_file=_config_file)

    def fetch(self, _obs_date=None):
        """

        :param _obs_date:
        :return:
        """
        try:
            # check if a new log file needs to be started
            self.check_logging()

            # download alerts

            # check if DB connection is alive/established
            connected = self.check_db_connection()

            if connected:
                pass

        except KeyboardInterrupt:
            # user ctrl-c'ed
            self.logger.error('User exited the fetcher.')
            # try disconnecting from the database (if connected)
            try:
                self.logger.info('Shutting down.')
                self.logger.debug('Cleaning tmp directory.')
                shutil.rmtree(self.config['path']['path_tmp'])
                os.makedirs(self.config['path']['path_tmp'])
                self.logger.debug('Disconnecting from DB.')
                self.disconnect_from_db()
            finally:
                self.logger.info('Bye!')
                return False

        except RuntimeError as e:
            # any other error not captured otherwise
            print(e)
            traceback.print_exc()
            self.logger.error(e)
            self.logger.error('Unknown error, exiting. Please check the logs.')
            try:
                self.logger.info('Shutting down.')
                self.logger.debug('Cleaning tmp directory.')
                shutil.rmtree(self.config['path']['path_tmp'])
                os.makedirs(self.config['path']['path_tmp'])
                self.logger.debug('Disconnecting from DB.')
                self.disconnect_from_db()
            finally:
                self.logger.info('Bye!')
                return False


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
                                     'Fetch AVRO packets from Archive/Kafka streams and ingest them into DB')
    parser.add_argument('config_file', help='path to config file')
    parser.add_argument('obsdate', help='observing date string: YYYYMMDD')

    args = parser.parse_args()
    obs_date = args.obsdate
    config_file = args.config_file

    f = FetcherArchive(config_file)
    f.fetch(obs_date)
