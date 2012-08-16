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

    def _get_current_user(self):
        auth = self.request.headers.get("Authorization")
        if auth:
          return decodestring(auth.split(" ")[-1]).split(":")

    def _load_request_as_json(self):
        return loads(self.request.body)

    def _load_sam_config(self):
        self.sam_settings = {'url': self.settings.sam_url, 'auth': [self.settings.sam_user, self.settings.sam_pass]}

    def __init__(self, *args, **kwargs):
        cyclone.web.RequestHandler.__init__(self, *args, **kwargs)
        self._load_sam_config()
        self._task_queue = self.settings.task_queue
        self.sam = Restfulie.at(self.sam_settings['url']).auth(*self.sam_settings['auth']).as_('application/json')

    @auth
    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def get(self):
        pass

    @auth
    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def post(self):
        request_as_json = self._load_request_as_json()
        doc = request_as_json['doc']
        key = self.sam.put(value={'doc':doc}).resource().key
        response = cyclone.web.escape.json_encode({'doc_key':key})
        self.set_header('Content-Type', 'application/json')
        self.finish(response)
