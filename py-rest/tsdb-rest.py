from tornado import ioloop
from tornado.web import Application, RequestHandler, asynchronous
from tornado.gen import coroutine, Task, engine
from tornado.httpserver import HTTPServer
from tornado.options import define, options
from logging import info
from redis import Redis, ConnectionPool
from os import environ
from json import dumps, loads
from datetime import datetime


# Reading config from env variables
REDIS_TSDB_DBNUMBER = int(environ.get('REDIS_TSDB_DBNUMBER', 6))
REDIS_TSDB_HOST = environ.get('REDIS_TSDB_HOST','192.168.1.100')
REDIS_TSDB_PORT = int(environ.get('REDIS_TSDB_PORT', 6385))
APP_URL = environ.get('APP_URL', 'http://localhost:18080/')

# Prepare RedisDB connection
rpool = ConnectionPool(
    host=REDIS_TSDB_HOST,
    port=REDIS_TSDB_PORT,
    db=REDIS_TSDB_DBNUMBER)

# Getting arguments from prompt line
define("port", default=18080, type=int, help=("Server port"))
define('debug', default=True, type=bool, help=("Turn on autoreload, log to stderr only"))
# define("config", default=None, help=("Tornado configuration file"))
options.parse_command_line()

settings = {
    'debug': options.debug,
    'port': options.port,
    'redis': Redis(connection_pool=rpool),
    'app_url': APP_URL
}

# Adicionar max & min


class BaseHandler(RequestHandler):
    def initialize(self):
        self.redis = self.settings['redis']

    def prepare(self):
        input_data = dict()
        self.input_data = dict()
        if self.request.method in ['POST', 'PUT']:
            try:
                if self.request.headers["Content-Type"].startswith(
                        "application/json") and self.request.body:
                    input_data = loads(self.request.body.decode("utf-8"))
                for k, v in self.request.arguments.items():
                    try:
                        input_data[k] = v[0].decode("utf-8")
                    except Exception as e:
                        info(e)
                        input_data[k] = v[0].decode("utf-8", errors='ignore')
                # Check TS points model
                # Keys: timestamps and values must be found in the dicts
                schema_keys = ('timestamps', 'values')
                if all([(key in input_data) for key in schema_keys]):
                    for key in schema_keys:
                        self.input_data[key] = input_data[key]
                    # Check datatypes
                    # * check timestamps and transform them if they are str
                    # info(self.input_data['timestamps'])
                    try:
                        if isinstance(self.input_data['timestamps'][0], str):
                            for n, v in enumerate(self.input_data['timestamps']):
                                self.input_data['timestamps'][n] = datetime.fromisoformat(v).timestamp()
                        self.input_data['timestamps'] = list(map(int, self.input_data['timestamps']))
                    except Exception as e:
                        info(e)
                        self.response(400, 'Error to convert date time information to timestamps')
                        return
                    # Check for numeric timestamps
                    ts_check = [(isinstance(tstamp, int) or isinstance(tstamp, float)) 
                        for tstamp in self.input_data['timestamps']]
                    if not all(ts_check):
                        self.response(400, 'Invalid timestamps.')
                        return
                    # * check values (numeric)
                    # Other types can be managed here
                    vals_check = [(isinstance(vals, int) or isinstance(vals, float)) 
                        for vals in self.input_data['values']]
                    if not all(vals_check):
                        self.response(400, 'Invalid values.')
                        return
                    if len(self.input_data['timestamps']) != len(self.input_data['values']):
                        self.response(400, 'Invalid number of timestamps and values.')
                else:
                    self.response(400, 'Invalid schema submitted.')
            except Exception as e:
                info(e)
                return self.response(
                    400, 'Fail to parse input data. It must be sent ' +
                    'with header Content-Type: application/json and ' +
                    'JSON serialized.')

    def set_default_headers(self):
        self.set_header('Content-Type', 'application/json; charset=UTF-8')

    def json_encode(self, value):
        return dumps(value, default=self.encoding).replace("</", "<\\/")
    
    def encoding(self, d):
        if isinstance(d, bytes):
            return d.decode('utf-8')
        elif isinstance(d, datetime):
            return d.isoformat()
        else:
            return str(d)

    def datetime_to_isoformat(self, object):
        try:
            if isinstance(object, list):
                return [self.datetime_to_isoformat(obj) for obj in object]
            elif isinstance(object, dict):
                return {key: self.datetime_to_isoformat(val) for key, val in object.items()}
            else:
                if isinstance(object, datetime):
                    try:
                        return object.isoformat()
                    except Exception as e:
                        info(e)
                        return object
                else:
                    return object
        except Exception as e:
            return object

    def response(self, code, message="", data=None,
            headers=None, parse=None):
        output_response = {'status': None, 'message': message}
        if parse:
            data = self.datetime_to_isoformat(data)
        if data:
            output_response['data'] = data
        if code < 300:
            output_response['status'] = 'success'
        elif code >= 300 and code < 400:
            output_response['status'] = 'redirect'
        elif code >= 400 and code < 500:
            output_response['status'] = 'error'
        else:
            output_response['status'] = 'fail'
        if headers and isinstance(headers, dict):
            for k, v in headers.items():
                self.add_header(k, v)
        self.set_status(code)
        self.finish(self.json_encode(output_response))

    @engine
    def redis_cmd(self, cmd, callback=None):
        self.redis.execute_command('keys ts*')
        callback(True)

    @engine
    def getPoints(self, tskey, start_ts, end_ts, aggrfunc='avg', aggrsec=86400, callback=None):
        resp = self.redis.execute_command(
            'TS.RANGE {} {} {} {} {}'.format(tskey, start_ts, end_ts, aggrfunc, aggrsec))
        # info(resp)
        output = {'timestamps': list(), 'values': list()}
        for v in resp:
            output['timestamps'].append(v[0])
            if '.' in str(v[1]):
                func = float
            else:
                func = int
            output['values'].append(func(v[1]))
        callback(output)

    @engine
    def addPoint(self, tskey, timestamp, value, callback=None):
        resp = self.redis.execute_command('TS.ADD {} {} {}'.format(tskey, timestamp, value))
        callback(resp)

    @engine
    def insert_points(self, tskey, timestamps, values, callback=None):
        results = {'success': 0, 'failures': 0}
        for n, v in enumerate(values):
            try:
                resp = yield Task(self.addPoint, tskey, timestamps[n], v)
                results['success'] += 1
            except:
                results['failures'] += 1
        callback(results)

    @engine
    def redis_keys(self, key_pattern, callback=None):
        callback(self.redis.keys('*{}*'.format(key_pattern)))

    @engine
    def create_key(self, tskey, callback=None):
        callback(
            self.redis.execute_command(
                'TS.CREATE {}'.format(tskey)))


class TSPointsHandler(BaseHandler):
    SUPPORTED_METHODS = ('GET', 'POST', 'DELETE')

    @asynchronous
    @coroutine
    def post(self, tskey=None):
        if tskey:
            # Check if the key exists
            key_exists = yield Task(self.redis_keys, tskey)
            # info(key_exists)
            if not any(key_exists):
                resp = yield Task(self.create_key, tskey)
                info('Key {} created: {}'.format(tskey ,resp))
            add_resp = yield Task(
                self.insert_points,
                tskey,
                self.input_data['timestamps'],
                self.input_data['values'])
            # ts81_0_0
            if add_resp.get('success') and add_resp.get('success') > 0:
                code = 201
            else:
                code = 200
            self.response(code, 'Points accepted to be added', add_resp)
        else:
            self.response(400, 'Invalid request. The POST requires a TS key.')

    @asynchronous
    @coroutine
    def get(self, tskey=None):
        if tskey:
            # Check if the key exists
            key_exists = yield Task(self.redis_keys, tskey)
            if not key_exists:
                self.response(404, 'Time series key not found.')
                return
            start = self.get_argument('start', 0)
            end = self.get_argument('end', int(datetime.now().timestamp()))
            tstp = self.get_argument('tstype', 'timestamp')
            aggrfunc = self.get_argument('aggr_func', 'avg')
            aggrsecs = self.get_argument('aggr_secs', 86400)
            resp = yield Task(self.getPoints, tskey, start, end, aggrfunc, aggrsecs)
            if 'datetime' in tstp:
                resp['timestamps'] = list(map(datetime.fromtimestamp, resp['timestamps']))
            resp['npoints'] = len(resp['values'])
            self.response(200, 'Points found for {}.'.format(tskey), resp)
        else:
            self.response(400, 'Invalid request. The POST requires a TS key.')


routes = [
    (r"/tsdb/(\w+)/?$", TSPointsHandler)
]

# Tornado application
class TornadoApplication(Application):
    def __init__(self):
        Application.__init__(self, routes, **settings)

# Run server
def main():
    app = TornadoApplication()
    info('====================================================================')
    info('Settings: ')
    for k, v in settings.items():
        info('  {} = {}'.format(k, v))
    info('====================================================================')
    info('Starting server listenning to port: ' + str(options.port))
    info('Handlers: ')
    for rt in routes:
        info('  {}'.format(str(rt)))
    info('====================================================================')
    httpserver = HTTPServer(app, xheaders=True)
    httpserver.listen(options.port)
    ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
