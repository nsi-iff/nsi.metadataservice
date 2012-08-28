from celery.task import Task
from celery.execute import send_task
from restfulie import Restfulie
from base64 import decodestring
from json import loads
from urllib import urlopen


class ExtractMetadata(Task):
    def run(self, key, filename, sam_settings):
        self.sam = Restfulie.at(sam_settings['url']).auth(*sam_settings['auth']).as_('application/json')
        temp_response = self.sam.get(key=key)
        old_dict = loads(temp_response.body)
        resource = temp_response.resource()
        tmp_doc_path = '/tmp/%s.%s' % (key, filename)
        open(tmp_doc_path, 'w+').write(decodestring(resource.data.file))
        metadata = self.find_metadata(tmp_doc_path)
        metadata_key = self.sam.put(value=metadata).resource().key
        metadata_dict = {'metadata_key': metadata_key}
        metadata_dict.update(old_dict)
        self.sam.post(key=key, value=metadata_dict)

    def find_metadata(self, tmp_doc_path):
        return {"autor":"Jose", "titulo":"A origem", "paginas":"55"}

