#!/usr/bin/env python

# Core modules
import os
import sys
import json

# Third-party modules
from bottle import *
from pykafka import KafkaClient


# API Exception wrapper for HTTPResponse
class APIError(Exception):
    def __init__(self, code, message, link=None):
        self.code = code
        self.message = message
        header = {'content-type': 'application/json'}
        if link is not None:
            body = { 'error': {'code':code,'message':message, 'href':link}}
        else:
            body = { 'error': {'code':code,'message':message}}
        raise HTTPResponse(json.dumps(body), code, header)

# Custom handling of 404
def custom404(error):
    msg = 'No endpoint resource found'
    body = json.dumps({ 'error': { 'code': 404, 'message': msg }})
    jsonheader = {'content-type': 'application/json'}
    return HTTPResponse(body, 404, jsonheader)


# Custom handling of 405
def custom405(error):
    msg = 'Method not allowed'
    body = json.dumps({ 'error': { 'code': 405, 'message': msg }})
    jsonheader = {'content-type': 'application/json'}
    return HTTPResponse(body, 405, jsonheader)


# Custom handling of 500
def custom500(error):
    return 'this is a 500'

def listjobs():
    consumer = topic.get_simple_consumer(consumer_timeout_ms=1000)
    jobs = {}
    for job in consumer:
        if job is not None:
            jobs[job.offset] = job.value
    return HTTPResponse(json.dumps(jobs), 200, {'content-type': 'application/json'})

def getjob(id):
    return "Job {}".format(id)

def addjob():
    return "Job added"

def deljob(id):
    return "Job {} deleted".format(id)

def getdata(id):
    return "Data for job {}: XXX".format(id)


# Initiate the API 
api = application = Bottle()
api.error_handler = { 404: custom404, 405: custom405, 500: custom500 }

# Try to locate the configuration file. 
cfile = '{}.conf'.format(os.path.splitext(os.path.basename(__file__))[0])
cdirs = ['.', '/etc', '/usr/local/etc']
for d in cdirs:
    config = '{}/{}'.format(d, cfile)
    if os.path.isfile(config):
        api.config.load_config(config)

# Connect to kafka
kafka = KafkaClient(hosts=api.config['kafka.hosts'])
topic = kafka.topics[api.config['kafka.topic']]

# Setup API routing
api.route('/v1/jobs',      'GET',    listjobs)
api.route('/v1/jobs',      'POST',   addjob)
api.route('/v1/jobs/<id>', 'GET',    getjob)
api.route('/v1/jobs/<id>', 'DELETE', deljob)
api.route('/v1/data/<id>', 'GET',    getdata)

# Run using WSGI server defined in config file if executed directly
if __name__ == "__main__":
    try:
        if api.config['wsgi.enabled']:
            api.run(server=api.config['wsgi.server'],
                    host=api.config['wsgi.host'],
                    port=api.config['wsgi.port'],
                    reloader=api.config['reloader'],
                    debug=api.config['debug'])
        else:
            exit('Enable WSGI in config, or run app with external WSGI server')
    except KeyError as e:
        exit('Configuration missing setting for {} in {}'.format(e, cfile))
