#!/usr/bin/env python

import os
import sys
import json
import yaml
import hashlib

from bottle import *
from iocparser import iocp
import customerror


# Context manager
class ListStream:
    def __init__(self):
        self.data = []
    def write(self, s):
        self.data.append(s)
    def __enter__(self):
        sys.stdout = self
        return self
    def __exit__(self, ext_type, exc_value, traceback):
        sys.stdout = sys.__stdout__


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


# Read configuration
with open("config.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

# Check upload path
upldir = cfg['upload']['path']
if upldir is None:
    exit('Upload path is not defined in config file')
if not os.path.exists(cfg['upload']['path']):
    os.makedirs(upldir)

# Check archive path
arcdir = cfg['archive']['path']
if arcdir is None:
    exit('Archive path is not defined in config file')
if not os.path.exists(cfg['archive']['path']):
    os.makedirs(arcdir)

# Check for route prefix
apiprefix = cfg['api']['routeprefix']
if apiprefix is None:
    apiprefix = 'http://{}:{}/'.format(cfg['wsgi']['host'], cfg['wsgi']['port'])


# Initiate the API with custom error handling
api = application = Bottle()
api.error_handler = customerror.handlers

# POST /v1/file
# Handle file uploads from multipart/form-data POST requests
@api.route('/v1/file', method='POST')
def fileparse():

    upload = request.files.get('file')
    if upload is None:
        msg = "Unable to process data. Couldn't find form name 'file'"
        raise APIError(400, msg)

    name, ext = os.path.splitext(upload.filename)
    if ext not in ('.pdf', '.txt', '.html'):
        raise APIError(406, 'You can only upload pdf, txt or html files')

    md5 = hashlib.md5(upload.file.getvalue()).hexdigest()
    ref = apiprefix + '/v1/ioc/' + md5
    if os.path.isfile('{}/{}'.format(arcdir, md5)):
        msg = 'This file has already been uploaded'
        raise APIError(409, msg, ref)

    upload.save(upldir)

    header = {'content-type': 'application/json'}
    msg = 'File uploaded and queued for parsing'
    body = { 'message': msg, 'href': ref }
    return HTTPResponse(json.dumps(body), 201, header)

# GET /v1/file/<id>
# Return the original file 
@api.route('/v1/file/<id>', method='GET')
def getfile(id):

    filepath = '{}/{}.json'.format(arcdir, id)
    if os.path.isfile(filepath) is False:
        raise APIError(404, 'No file found with that id')

    with open(filepath) as data_file:
        data = json.load(data_file)
    origname = data['meta']['filename']

    return static_file(origname, root=arcdir)

# GET /v1/file
# Return the collection of parsed files

# GET /v1/ioc/<id>
# Return the parsed IOCs from the file with id

# Run using WSGI server defined in config file if executed directly
if __name__ == "__main__":
    wsgi = cfg['wsgi']
    if wsgi['enabled'] is True:
        api.run( reloader=wsgi['devmode'],
                 server=wsgi['server'],
                 host=wsgi['host'],
                 port=wsgi['port'] )
    else:
        exit('Enable WSGI in config, or run this using an external WSGI server')
