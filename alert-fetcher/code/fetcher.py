import argparse
import pymongo
import os
import glob
import inspect
import json
import logging
import datetime
import fastavro
import time
import shutil
import traceback
import requests
from progress.bar import Bar
from progress.spinner import Spinner
import tarfile
import numpy as np
import pytz
from numba import jit


def utc_now():
    return datetime.datetime.now(pytz.utc)


def time_stamps():
    """

    :return: local time, UTC time
    """
    return datetime.datetime.now().strftime('%Y%m%d_%H:%M:%S'), \
           datetime.datetime.utcnow().strftime('%Y%m%d_%H:%M:%S')


@jit
def deg2hms(x):
    """Transform degrees to *hours:minutes:seconds* strings.

    Parameters
    ----------
    x : float
        The degree value c [0, 360) to be written as a sexagesimal string.

    Returns
    -------
    out : str
        The input angle written as a sexagesimal string, in the
        form, hours:minutes:seconds.

    """
    assert 0.0 <= x < 360.0, 'Bad RA value in degrees'
    # ac = Angle(x, unit='degree')
    # hms = str(ac.to_string(unit='hour', sep=':', pad=True))
    # print(str(hms))
    _h = np.floor(x * 12.0 / 180.)
    _m = np.floor((x * 12.0 / 180. - _h) * 60.0)
    _s = ((x * 12.0 / 180. - _h) * 60.0 - _m) * 60.0
    hms = '{:02.0f}:{:02.0f}:{:07.4f}'.format(_h, _m, _s)
    # print(hms)
    return hms


@jit
def deg2dms(x):
    """Transform degrees to *degrees:arcminutes:arcseconds* strings.

    Parameters
    ----------
    x : float
        The degree value c [-90, 90] to be converted.

    Returns
    -------
    out : str
        The input angle as a string, written as degrees:minutes:seconds.

    """
    assert -90.0 <= x <= 90.0, 'Bad Dec value in degrees'
    # ac = Angle(x, unit='degree')
    # dms = str(ac.to_string(unit='degree', sep=':', pad=True))
    # print(dms)
    _d = np.floor(abs(x)) * np.sign(x)
    _m = np.floor(np.abs(x - _d) * 60.0)
    _s = np.abs(np.abs(x - _d) * 60.0 - _m) * 60.0
    dms = '{:02.0f}:{:02.0f}:{:06.3f}'.format(_d, _m, _s)
    # print(dms)
    return dms


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

    def insert_db_entry(self, _collection=None, _db_entry=None):
        """
            Insert a document _doc to collection _collection in DB.
            It is monitored for timeout in case DB connection hangs for some reason
        :param _collection:
        :param _db_entry:
        :return:
        """
        assert _collection is not None, 'Must specify collection'
        assert _db_entry is not None, 'Must specify document'
        try:
            self.db['db'][_collection].insert_one(_db_entry)
        except Exception as _e:
            print(*time_stamps(), 'Error inserting {:s} into {:s}'.format(str(_db_entry['_id']), _collection))
            traceback.print_exc()
            print(_e)

    def insert_multiple_db_entries(self, _collection=None, _db_entries=None):
        """
            Insert a document _doc to collection _collection in DB.
            It is monitored for timeout in case DB connection hangs for some reason
        :param _db:
        :param _collection:
        :param _db_entries:
        :return:
        """
        assert _collection is not None, 'Must specify collection'
        assert _db_entries is not None, 'Must specify documents'
        try:
            # ordered=False ensures that every insert operation will be attempted
            # so that if, e.g., a document already exists, it will be simply skipped
            self.db['db'][_collection].insert_many(_db_entries, ordered=False)
        except pymongo.errors.BulkWriteError as bwe:
            print(*time_stamps(), bwe.details)
        except Exception as _e:
            traceback.print_exc()
            print(_e)

    @staticmethod
    def alert_mongify(alert):

        doc = dict(alert)

        # candid+objectId should be a unique combination:
        doc['_id'] = f"{alert['candid']}_{alert['objectId']}"

        # GeoJSON for 2D indexing
        doc['coordinates'] = {}
        doc['coordinates']['epoch'] = doc['candidate']['jd']
        _ra = doc['candidate']['ra']
        _dec = doc['candidate']['dec']
        _radec = [_ra, _dec]
        # string format: H:M:S, D:M:S
        # tic = time.time()
        _radec_str = [deg2hms(_ra), deg2dms(_dec)]
        # print(time.time() - tic)
        # print(_radec_str)
        doc['coordinates']['radec_str'] = _radec_str
        # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
        _radec_geojson = [_ra - 180.0, _dec]
        doc['coordinates']['radec_geojson'] = {'type': 'Point',
                                               'coordinates': _radec_geojson}
        # radians:
        doc['coordinates']['radec'] = [_ra * np.pi / 180.0, _dec * np.pi / 180.0]

        return doc

    def fetch(self, **kwargs):
        """

        """
        raise NotImplementedError


class FetcherKafka(Fetcher):
    """
        Fetch ZTF alerts from Kafka
    """
    def __init__(self, _config_file):
        """

        :param _config_file:
        """

        ''' initialize super class '''
        super(FetcherKafka, self).__init__(_config_file=_config_file)

    def fetch(self, **kwargs):
        pass


class FetcherArchive(Fetcher):
    """
        Fetch ZTF alerts from the archive
    """
    def __init__(self, _config_file):
        """

        :param _config_file:
        """

        ''' initialize super class '''
        super(FetcherArchive, self).__init__(_config_file=_config_file)

        ''' db stuff '''
        # number of records to insert to db
        self.batch_size = int(self.config['misc']['batch_size'])
        self.documents = []

    def fetch(self, _obs_date=None, _demo=False):
        """

        :param _obs_date:
        :param _demo:
        :return:
        """
        assert _obs_date is not None, 'must specify obs date'

        try:
            # check if a new log file needs to be started
            self.check_logging()

            # download alerts

            # check if DB connection is alive/established
            connected = self.check_db_connection()

            if connected:
                # fetch
                if not _demo:
                    url = os.path.join(self.config['misc']['ztf_public_archive'], f'ztf_public_{_obs_date}.tar.gz')
                else:
                    # fetch demo from skipper
                    _obs_date = self.config['misc']['demo']['date']
                    url = self.config['misc']['demo']['url']

                file_name = os.path.join(self.config['path']['path_alerts'], f'{_obs_date}.tar.gz')

                if not os.path.exists(file_name):
                    print(f'Fetching {url}')

                    r = requests.get(url, stream=True)

                    size = r.headers['content-length']
                    if size:
                        p = Bar(_obs_date, max=int(size))
                    else:
                        p = Spinner(_obs_date)

                    with open(file_name, 'wb') as _f:
                        for chunk in r.iter_content(chunk_size=1024 * 50):
                            if chunk:  # filter out keep-alive new chunks
                                p.next(len(chunk))
                                _f.write(chunk)

                    p.finish()

                    # unzip:
                    print(f'Unzipping {file_name}')
                    with tarfile.open(file_name) as tf:
                        path_date = os.path.join(self.config['path']['path_alerts'], f'{_obs_date}')
                        if not os.path.exists(path_date):
                            os.makedirs(path_date)
                        tf.extractall(path=path_date)

                    # ingest into db:
                    print(f'Ingesting {_obs_date} into db')
                    alerts_date = glob.glob(os.path.join(self.config['path']['path_alerts'],
                                                         f'{_obs_date}', '*.avro'))
                    for fa in alerts_date:
                        try:
                            with open(fa, 'rb') as f_avro:
                                reader = fastavro.reader(f_avro)
                                for record in reader:
                                    alert = self.alert_mongify(record)
                                    # print('ingesting {:s} into db'.format(alert['_id']))
                                    # self.insert_db_entry(_collection=self.config['database']['collection_alerts'],
                                    #                      _db_entry=alert)
                                    self.documents.append(alert)

                            # insert batch, then flush
                            if len(self.documents) == self.batch_size:
                                print(f'inserting batch')
                                self.logger.info(f'inserting batch')
                                self.insert_multiple_db_entries(_collection=
                                                                self.config['database']['collection_alerts'],
                                                                _db_entries=self.documents)
                                # flush:
                                self.documents = []
                        except Exception as _e:
                            print(_e)
                            traceback.print_exc()
                            continue

                    # stuff left in the last batch?
                    if len(self.documents) > 0:
                        print(f'inserting last batch')
                        self.logger.info(f'inserting last batch')
                        self.insert_multiple_db_entries(_collection=self.config['database']['collection_alerts'],
                                                        _db_entries=self.documents)
                        self.documents = []

                    # creating alert id index:
                    print('Creating 1d indices')
                    self.db['db'][self.config['database']['collection_alerts']].create_index([('objectId', 1)])
                    self.db['db'][self.config['database']['collection_alerts']].create_index([('candid', 1)])
                    self.db['db'][self.config['database']['collection_alerts']].create_index([('candidate.rb', 1)])
                    self.db['db'][self.config['database']['collection_alerts']].create_index([('candidate.fwhm', 1)])
                    self.db['db'][self.config['database']['collection_alerts']].create_index([('candidate.field', 1)])
                    self.db['db'][self.config['database']['collection_alerts']].create_index([('candidate.magpsf', 1)])
                    self.db['db'][self.config['database']['collection_alerts']].create_index([('candidate.jd', 1)])

                    # create 2d index:
                    print('Creating 2d index')
                    self.db['db'][self.config['database']['collection_alerts']].create_index([('coordinates.radec_geojson',
                                                                                               '2dsphere')])
                    print('All done')

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
    parser.add_argument('--demo', action='store_true', help='fetch demo alerts?')

    args = parser.parse_args()
    obs_date = args.obsdate
    config_file = args.config_file
    demo = args.demo

    f = FetcherArchive(config_file)
    f.fetch(obs_date, demo)
