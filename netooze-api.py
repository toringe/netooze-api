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
from addict import Dict
from hashids import Hashids
from pykafka import KafkaClient, exceptions
from elasticsearch import *

STATUS = Dict()
STATUS.Q = "queued"
STATUS.P = "processing"
STATUS.F = "finished"

JSONHDR = {'content-type': 'application/json'}


# API Exception wrapper for HTTPResponse
class APIError(Exception):
    def __init__(self, code, message, link=None):
        self.code = code
        self.message = message
        if link is not None:
            body = {'error': {'code': code, 'message': message, 'href': link}}
        else:
            body = {'error': {'code': code, 'message': message}}
        raise HTTPResponse(json.dumps(body), code, JSONHDR)


# Custom handling of 404
def custom404(error):
    body = {'error': {'code': 404, 'message': 'No endpoint resource found'}}
    return HTTPResponse(json.dumps(body), 404, JSONHDR)


# Custom handling of 405
def custom405(error):
    body = {'error': {'code': 405, 'message': 'Method not allowed'}}
    return HTTPResponse(json.dumps(body), 405, JSONHDR)


# Custom handling of 500
def custom500(error):
    return 'this is a 500'


def getjob(inputs):
    """ Get meta data about jobs for the given user. Either a list of all
        jobs for this user, or filtering can be done by providing a spesific
        jobid or job status.
    """
    # Split the input path: /jobs/<user>(/<id|finshed|queued|processing>)
    fields = inputs.rstrip('/').split('/')
    user = fields[0]
    hashids = Hashids(salt=user)
    if len(fields) > 1:
        # User has specified a filter, need to determine if its status or id
        query = 'match'
        f = fields[1]
        if f.lower() in STATUS.values():
            key = 'status'
            value = f.lower()
        else:
            key = 'id'
            h = hashids.decode(f)
            if len(h) == 0:
                return APIError(404, 'No such job exists')
            else:
                value = h[0]
    else:
        # User has not provided a filter, showing all jobs
        query = 'wildcard'
        key = 'status'
        value = '*'

    # Search in ES
    search = {"query": {query: {key: value}}}
    res = es.search(index=indexname, doc_type=user, body=search)
    records = res['hits']['hits']
    if len(records) == 0:
        return APIError(404, 'No such job exists')

    # Create the json result which will be returned to the user
    if key == 'id':
        rec = records[0]['_source']
        rec.pop('id')  # Remove the actual ES id
        return rec
    else:
        jobs = Dict()
        for data in records:
            rec = data['_source']
            jobid = hashids.encode(rec['id'])
            jobs[jobid].jobid = jobid
            jobs[jobid].created = rec['timestamp']
            jobs[jobid].desc = rec['desc']
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
        # Get max id and increment it.
        query = {'aggs': {'max_id': {'max': {'field': 'id'}}}, 'size': 0}
        res = es.search(index=indexname, doc_type=user, body=query)
        maxid = res['aggregations']['max_id']['value']
        if maxid is None:
            maxid = 0
        esid = int(maxid) + 1

        # Create the job spesification
        job = Dict()
        job.id = esid
        job.user = user
        job.timestamp = datetime.now().isoformat()
        job.client = data['client']
        job.host = data['host']
        job.desc = data['desc']
        job.status = STATUS.Q
        job.query = json.loads(data['query'])
        job.notes = ""
        job.priority = 0
        job.options = {}
        if 'options' in data:
            job.options = json.loads(data['options'])

        # Add to ES
        res = es.create(index=indexname, doc_type=user, body=job, id=esid)

    except ElasticsearchException, e:
        raise APIError(503, 'Connection to Elasticsearch failed {}'.format(e))

    # Successful insert to ES
    if res['created']:
        # Now try to insert id to Kafka
        kafkaid = '{}:{}'.format(user, esid)
        try:
            producer = topic.get_producer(min_queued_messages=1)
            producer.produce(kafkaid)
            producer.stop()
        except KafkaException:
            errmsg = 'Kafka producer failed to insert into'
            raise APIError(502, '{}: {}'.format(errmsg, topic.name))

        # Finally return the job id to the user (a hashed representation)
        response.status = 201
        hashid = Hashids(salt=user)
        hid = hashid.encode(esid)
        message = Dict()
        message.id = hid
        message.desc = data['desc']
        message.status = STATUS.Q
        return message


def deljob(user, jobid):
    """ Delete a job with given id for given user. """
    # Decode jobid
    hashid = Hashids(salt=user)
    hid = hashid.decode(jobid)
    if len(hid) == 0:
        return APIError(404, 'No such job exists')

    # Check if the job actually exists
    q = {'query': {'match': {'id': hid[0]}}}
    res = es.search(index=indexname, doc_type=user, body=q)
    records = res['hits']['hits']
    if len(records) == 1:
        res = es.delete(index=indexname, doc_type=user, id=hid[0])
        if res['found']:
            return None
        else:
            raise APIError(502, 'Deletion failed')
    else:
        raise APIError(404, 'No such job exists')


def getdata(user, jobid):
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
    logger.warning('No elastic index called {} was found'.format(indexname))
    conf = Dict()
    conf.settings.number_of_shards = api.config['elasticsearch.shards']
    conf.settings.number_of_replicas = api.config['elasticsearch.replicas']
    try:
        es.indices.create(index=indexname, body=conf)
        logger.info('Created index: {}'.format(indexname))
    except ElasticsearchException, e:
        if es.indices.exists(indexname):
            logger.warning('Doh...index was created by a faster thread')
        else:
            logger.error('Exception: {}'.format(str(e)))

# Setup API routing
api.route('/v1/jobs/<inputs:path>',  'GET',    getjob)
api.route('/v1/jobs/<user>',         'POST',   addjob)
api.route('/v1/jobs/<user>/<jobid>', 'DELETE', deljob)
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
