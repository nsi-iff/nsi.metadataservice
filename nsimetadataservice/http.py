#!/usr/bin/env python
#-*- coding:utf-8 -*-

from json import dumps, loads
from base64 import decodestring, b64encode
import functools
import cyclone.web
from twisted.internet import defer
from twisted.python import log
from zope.interface import implements
from nsimetadataservice.interfaces.http import IHttp
from restfulie import Restfulie
from celery.execute import send_task
from urlparse import urlsplit

def auth(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        auth_type, auth_data = self.request.headers.get("Authorization").split()
        if not auth_type == "Basic":
            raise cyclone.web.HTTPAuthenticationRequired("Basic", realm="Restricted Access")
        user, password = decodestring(auth_data).split(":")
        # authentication itself
        if not self.settings.auth.authenticate(user, password):
            log.msg("Authentication failed.")
            log.msg("User '%s' and password '%s' not known." % (user, password))
            raise cyclone.web.HTTPError(401, "Unauthorized")
        return method(self, *args, **kwargs)
    return wrapper


class HttpHandler(cyclone.web.RequestHandler):

    implements(IHttp)
    no_keep_alive = True

    def _verify_errors(self, response, key=None):
        if response.code == '500':
            log.msg("GET failed!")
            log.msg("There is an unexpected exception.")
            raise cyclone.web.HTTPError(500, 'Unexpected exception.')
        if response.code == "400":
            log.msg("GET failed!")
            log.msg("Request didn't have a key to find.")
            raise cyclone.web.HTTPError(400, 'Malformed request.')
        if response.code == "401":
            log.msg("GET failed!")
            log.msg("METADATASERVICE user and password not match.")
            raise cyclone.web.HTTPError(401, 'METADATASERVICE user and password not match.')
        if response.code == "404":
            log.msg("GET failed!")
            log.msg("Couldn't find any value for the key: %s" % key)
            raise cyclone.web.HTTPError(404, 'Unknown key.')

    def _get_current_user(self):
        auth = self.request.headers.get("Authorization")
        if auth:
          return decodestring(auth.split(" ")[-1]).split(":")

    def _load_request_as_json(self):
        return loads(self.request.body)

    def _load_sam_config(self):
        self.sam_settings = {'url': self.settings.sam_url, 'auth': [self.settings.sam_user, self.settings.sam_pass]}

    def _enqueue_document(self, key, filename, sam_settings):
        send_task('nsimetadataservice.tasks.ExtractMetadata', args=(key, filename,
                   sam_settings), queue=self._task_queue, routing_key=self._task_queue)

    def __init__(self, *args, **kwargs):
        cyclone.web.RequestHandler.__init__(self, *args, **kwargs)
        self._load_sam_config()
        self._task_queue = self.settings.task_queue
        self.sam = Restfulie.at(self.sam_settings['url']).auth(*self.sam_settings['auth']).as_('application/json')

    @auth
    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def get(self):
        key = self._load_request_as_json().get('key')
        if not key:
            log.msg("GET failed!")
            log.msg("Request didn't have a key to find.")
            raise cyclone.web.HTTPError(400, 'Malformed request.')
        is_metadata_request = self._load_request_as_json().get('metadata')
        response = yield self.sam.get(key=key)
        self._verify_errors(response, key)
        log.msg("Request to SAM processed successfully")
        response = response.resource()
        if hasattr(response.data, 'metadata_key'):
            if is_metadata_request:
                log.msg("The metadata documents are stored and a metadata_key attribute was created.")
                response = cyclone.web.escape.json_encode({'metadata_key':response.data.metadata_key})
            else:
                log.msg("Data extraction was completed successfully.")
                response = cyclone.web.escape.json_encode({'done':True})
        else:
            log.msg("Object 'response.data' didn't have 'metadata_key' attribute.")
            response = cyclone.web.escape.json_encode({'done':False})
        self.set_header('Content-Type', 'application/json')
        self.finish(response)

    @auth
    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def post(self):
        request_as_json = yield self._load_request_as_json()
        doc = request_as_json.get('doc')
        filename = request_as_json.get('filename')
        if not doc or not filename:
            log.msg("POST failed!")
            log.msg("Filename and document unknown.")
            raise cyclone.web.HTTPError(400, 'Malformed request.')
        response = self.sam.put(value={'doc':doc})
        self._verify_errors(response)
        log.msg('Request to SAM processed successfully')
        key = response.resource().key
        response = cyclone.web.escape.json_encode({'doc_key':key})
        self.set_header('Content-Type', 'application/json')
        self._enqueue_document(key, filename, self.sam_settings)
        self.finish(response)
