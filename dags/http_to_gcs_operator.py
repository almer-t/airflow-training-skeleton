from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

from urllib.parse import urlparse, urlunparse

import tempfile

# Curl reference:
# https://europe-west1-gdd-airflow-training.cloudfunctions.net/airflow-training-transform-valutas?date=1970-01-01&from=GBP&to=EUR

class HttpToGcsOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action

    :param http_conn_id: The connection to run the operator against
    :type http_conn_id: string
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: string
    :param gcs_path: The path of the GCS to store the result
    :type gcs_path: string
    """

    template_fields = ('endpoint', 'gcs_path')
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 endpoint, gcs_path,
                 http_conn_id="http_default",
                 gcs_conn_id="gcs_default"
                 *args, **kwargs):
        super(HttpToGcsOperator, self).__init__(*args, **kwargs)

        self.http_conn_id = http_conn_id
        self.gcs_conn_id = gcs_conn_id
        self.gcs_path = gcs_path
        self.endpoint = endpoint

    def execute(self, context):

        self.http_hook = HttpHook(self.http_conn_id, method='GET')
        self.gcs_hook = GoogleCloudStorageHook(self.gcs_conn_id)

        bucket, blob = self.gcs_hook._parse_gcs_url(self.gcs_path)

        # Parse the query into components, extract query part
        parsed = urlparse(self.endpoint)
        base_url = urlunparse(list(parsed[:4]) + ["", ""])

        # Create temporary
        with tempfile.NamedTemporaryFile() as fp:
            # Get response
            response = self.http_hook.run(base_url, date=parsed.query)
            fp.write(response)
            fp.close()

            # Upload the file to storage
            self.gcs_hook.upload(bucket, blob, filename=fp.name)
