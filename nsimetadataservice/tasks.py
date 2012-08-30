from celery.task import Task
from celery.execute import send_task
from restfulie import Restfulie
from base64 import decodestring
from json import loads

class ExtractMetadata(Task):

    def _send_callback_task(self):
        send_task('nsimetadataservice.tasks.Callback', 
                   args=(self._callback_url, self._callback_verb, self._doc_key, self._metadata_key),
                   queue=self._task_queue, routing_key=self._routing_key)


    def run(self, doc_key, filename, sam_settings, callback_url, callback_verb, task_queue, routing_key):
        self._doc_key = doc_key
        self._sam_settings = sam_settings
        self._callback_url = callback_url
        self._callback_verb = callback_verb
        self._task_queue = task_queue
        self._routing_key = routing_key
        self._sam = Restfulie.at(sam_settings['url']).auth(*sam_settings['auth']).as_('application/json')
        temp_response = self._sam.get(key=doc_key)
        old_dict = loads(temp_response.body)
        resource = temp_response.resource()
        tmp_doc_path = '/tmp/%s.%s' % (doc_key, filename)
        open(tmp_doc_path, 'w+').write(decodestring(resource.data.file))
        metadata = self._find_metadata(tmp_doc_path)
        if metadata:
            metadata_key = self._sam.put(value=metadata).resource().key
            metadata_dict = {'metadata_key': metadata_key}
            self._metadata_key = metadata_key
            metadata_dict.update(old_dict)
            self._sam.post(key=doc_key, value=metadata_dict)
            if self._callback_verb:
                self._send_callback_task()

    def _find_metadata(self, tmp_doc_path):
        return {"autor":"Jose", "titulo":"A origem", "paginas":"55"}


class Callback(Task):

    def run(self, url, verb, doc_key, metadata_key):
        try:
            print "Sending callback to %s" % url
            restfulie = Restfulie.at(url).as_('application/json')
            response = getattr(restfulie, verb.lower())(doc_key=doc_key, metadata_key=metadata_key,
                                                        done=True)
        except Exception, e:
            Callback.retry(exc=e, countdown=10)
        else:
            print "Callback executed."
            print "Response code: %s" % response.code