#!/usr/bin/env python

# Core modules
import os
import sys
import json
import getpass
import logging
from datetime import datetime
from logging.handlers import SysLogHandler

# Third-party modules
from bottle import *
from hashids import Hashids
from pykafka import KafkaClient, exceptions
from elasticsearch import *


# API Exception wrapper for HTTPResponse
class APIError(Exception):
    def __init__(self, code, message, link=None):
        self.code = code
        self.message = message
        header = {'content-type': 'application/json'}
        if link is not None:
            body = {'error': {'code': code, 'message': message, 'href': link}}
        else:
            body = {'error': {'code': code, 'message': message}}
        raise HTTPResponse(json.dumps(body), code, header)


#TODO: Could try to use the error decorator instead..
# Custom handling of 404
def custom404(error):
    msg = 'No endpoint resource found'
    body = json.dumps({'error': {'code': 404, 'message': msg}})
    jsonheader = {'content-type': 'application/json'}
    return HTTPResponse(body, 404, jsonheader)


# Custom handling of 405
def custom405(error):
    msg = 'Method not allowed'
    body = json.dumps({'error': {'code': 405, 'message': msg}})
    jsonheader = {'content-type': 'application/json'}
    return HTTPResponse(body, 405, jsonheader)


# Custom handling of 500
def custom500(error):
    return 'this is a 500'


def getjob(data):
    """ Get meta data about jobs for the given user. Either a list of all
        jobs for this user, or filtering can be done by providing a spesific
        jobid or job status.
    """
    # Split the input path: /jobs/<user>(/<id|finshed|queued>)
    fields = data.rstrip('/').split('/')
    user = fields[0]
    hashids = Hashids(salt=user)
    if len(fields) > 1:
        # User has specified a filter, need to determine if its status or id
        query = 'match'
        if fields[1].lower() in ['finished', 'queued']:
            key = 'status'
            value = fields[1].lower()
        else:
            key = 'id'
            value = hashids.decode(fields[1])[0]
    else:
        # User has not provided a filter, showing all jobs
        query = 'wildcard'
        key = 'status'
        value = '*'

    # Search in ES
    search = {'query': {query: {key: value}}}
    res = es.search(index=indexname, doc_type=user, body=search)
    records = res['hits']['hits']

    # Create the json result returned to the user
    jobs = {}
    for rec in records:
        r = rec['_source']
        i = hashids.encode(r['id'])
        jobs[i] = {'jobid': i, 'created': r['timestamp'], 'desc': r['desc']}
    return jobs


def addjob(user):
    """ Create a new job spec for this user. This job details will be put in
        Elasticsearch and the job-id in Kafka for further processing.
        :user: Username as provided in the path of the POST request
    """
    # The following POST fields are mandatory so check that they are provided
    reqfields = ['client', 'host', 'desc', 'query']
    data = request.json
    for field in reqfields:
        if field not in data:
            raise APIError(422, 'Missing required field: {}'.format(field))
    try:
        # All is ok. Increment document ID and try to insert into ES.
        esid = es.count(indexname, user)['count'] + 1
        job = {'id': esid,
               'user': user,
               'timestamp': datetime.now().isoformat(),
               'client': data['client'],
               'host': data['host'],
               'desc': data['desc'],
               'query': data['query'],
               'status': 'queued'
               }
        res = es.create(indexname, user, job, esid)
    except ElasticsearchException:
        raise APIError(503, 'Connection to Elasticsearch failed')

    if res['created']:
        # Successful insert to ES. Now try to insert id to Kafka
        kafkaid = '{}:{}'.format(user, esid)
        try:
            producer = topic.get_producer(min_queued_messages=1)
            producer.produce(kafkaid)
            producer.stop()
        except KafkaException:
            errmsg = 'Kafka producer failed to insert into'
            raise APIError(503, '{}: {}'.format(errmsg, topic.name))

        # Finally return the job id to the user (a hashed representation)
        response.status = 201
        hashid = Hashids(salt=user)
        hid = hashid.encode(esid)
        message = {"id": hid, "desc": data['desc'], "status": "queued"}
        return message


def deljob(user, id):
    raise APIError(501, 'Deletion not yet implemented')


def getdata(id):
    raise APIError(501, 'Data retrival not yet implemented')


# Who am I?
appname = os.path.splitext(os.path.basename(__file__))[0]

# Initiate the API
api = application = Bottle()
api.error_handler = {404: custom404, 405: custom405, 500: custom500}

# Try to locate the configuration file.
cfile = '{}.conf'.format(appname)
cdirs = ['.', '/etc', '/usr/local/etc']
for d in cdirs:
    config = '{}/{}'.format(d, cfile)
    if os.path.isfile(config):
        api.config.load_config(config)

# Log to local syslog
logger = logging.getLogger(appname)
logger.setLevel(logging.INFO)
syslog = SysLogHandler(address='/dev/log')
formatter = logging.Formatter('%(name)s: <%(levelname)s> -  %(message)s')
syslog.setFormatter(formatter)
logger.addHandler(syslog)

# Connect to kafka
kafkahosts = api.config['kafka.hosts']
try:
    kafka = KafkaClient(hosts=kafkahosts)
except RuntimeError:
    errmsg = 'Failed to connect to Kafka on {}'.format(kafkahosts)
    logger.error(errmsg)
    exit(errmsg)
else:
    logger.info('Connected to Kafka on {}'.format(kafkahosts))
finally:
    topic = kafka.topics[api.config['kafka.topic']]

# Connect to elasticsearch
eshosts = api.config['elasticsearch.hosts']
es = Elasticsearch(eshosts.split())
if not es.ping():
    errmsg = 'Failed to connect to elasticsearch on {}'.format(eshosts)
    logger.error(errmsg)
    exit(errmsg)
else:
    logger.info('Connected to elasticsearch on {}'.format(eshosts))

# Create index if not existing
indexname = api.config['elasticsearch.index']
if not es.indices.exists(indexname):
    logger.warning('No elastic index found. Creating {}'.format(indexname))
    custombody = {'settings': {
                  'number_of_shards': api.config['elasticsearch.shards'],
                  'number_of_replicas': api.config['elasticsearch.replicas']}}
    es.indices.create(index=indexname, body=custombody)

# Setup API routing
api.route('/v1/jobs/<data:path>',  'GET',    getjob)
api.route('/v1/jobs/<user>',       'POST',   addjob)
#api.route('/v1/jobs/<user>/<id>', 'DELETE', deljob)
#api.route('/v1/data/<user>/<id>', 'GET',    getdata)

if __name__ == "__main__":
    # App is running by itself, thus we need to have a WSGI server defined
    # in our configuration file.
    try:
        if api.config['wsgi.enabled']:
            api.run(server=api.config['wsgi.server'],
                    host=api.config['wsgi.host'],
                    port=api.config['wsgi.port'],
                    debug=api.config['debug'])
        else:
            exit('Enable WSGI in config, or run app with external WSGI server')
    except KeyError as e:
        exit('Configuration missing setting for {} in {}'.format(e, cfile))
