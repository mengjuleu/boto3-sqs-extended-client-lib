import random
import string

from unittest import TestCase
from unittest.mock import MagicMock
from sqs_client.SQSClientExtended import SQSClientExtended


class SQSMessageMock(object):
    def __init__(self):
        """
        Imitate the SQS Message from boto3.
        """
        self.body = ""
        self.receipt_handle = "receipt_handle_xyz"


class QueueMock(object):
    """ Hold information about a queue. """

    def __init__(self, url):
        self.url = url
        self.attributes = {'ApproximateNumberOfMessages': '0'}

        self.messages = []

    def __repr__(self):
        return 'QueueMock: {} {} messages'.format(self.url, len(self.messages))


class SQSClientMock(object):
    SQS_SIZE_LIMIT = 262144

    def __init__(self):
        """
        Imitate the SQS Client from boto3.
        """
        self._receive_messages_calls = 0
        # _queues doesn't exist on the real client, here for testing.
        self._queues = {}
        for n in range(1):
            name = 'q_{}'.format(n)
            self.create_queue(QueueName=name)

        url = self.create_queue(QueueName='unittest_queue')['QueueUrl']
        #self.send_message(QueueUrl=url, MessageBody='hello')

    def _get_q(self, url):
        """ Helper method to quickly get a queue. """
        for q in self._queues.values():
            if q.url == url:
                return q
        raise Exception("Queue url {} not found".format(url))

    def create_queue(self, **kwargs):
        queue_name = kwargs.get('QueueName')

        q = self._queues[queue_name] = QueueMock('sqs://{}'.format(queue_name))
        return {'QueueUrl': q.url}

    def get_queue_url(self, **kwargs):
        queue_name = kwargs.get('QueueName')
        return self._queues[queue_name]

    def send_message(self, **kwargs):
        queue_url = kwargs.get('QueueUrl')
        message_body = kwargs.get('MessageBody')
        message_attributes = kwargs.get('MessageAttributes')

        if message_attributes is None:
            message_attributes = {}

        if len(message_body) > self.SQS_SIZE_LIMIT:
            raise ValueError('Message payload size is pover {}'.format(self.SQS_SIZE_LIMIT))

        print(self._queues)

        for q in self._queues.values():
            if q.url == 'sqs://{}'.format(queue_url):
                handle = ''.join(random.choice(string.ascii_lowercase) for
                                 _ in range(10))
                q.messages.append({'Body': message_body,
                                   'ReceiptHandle': handle,
                                   'MessageAttributes': message_attributes})
                break

    def receive_message(self, **kwargs):
        queue_url = kwargs.get('QueueUrl')
        max_num_of_messages = kwargs.get('MaxNumberOfMessages') or 1
        self._receive_messages_calls += 1
        for q in self._queues.values():
            if q.url == queue_url:
                msgs = q.messages[:max_num_of_messages]
                q.messages = q.messages[max_num_of_messages:]
                return {'Messages': msgs} if msgs else {}

    def get_queue_attributes(self, **kwargs):
        queue_url = kwargs.get('QueueUrl')
        attribute_names = kwargs.get('AttributeNames')
        if 'ApproximateNumberOfMessages' in attribute_names:
            count = len(self._get_q(queue_url).messages)
            return {'Attributes': {'ApproximateNumberOfMessages': count}}

    def purge_queue(self, **kwargs):
        queue_url = kwargs.get('QueueUrl')
        for q in self._queues.values():
            if q.url == queue_url:
                q.messages = []

    def peek_message(self, **kwargs):
        queue_url = kwargs.get('QueueUrl')

        for q in self._queues.values():
            if q.url == 'sqs://{}'.format(queue_url):
                print(q.messages)
                if q.messages:
                    return q.messages[-1]
        return None


class S3Object:
    class StringReader:
        def __init__(self, content):
            self._content = content

        def read(self):
            return self._content

    def __init__(self, key, body, bucket):
        self._key = key
        self._body = body
        self._bucket = bucket

    def get(self):
        return {'Body': S3Object.StringReader(self._body)}

    def delete(self):
        self._bucket.delete(self._key)


class S3Bucket:
    def __init__(self, name):
        self._name = name
        self._objects = {}

    def put_object(self, **kwargs):
        key = kwargs.get('Key')
        body = kwargs.get('Body')
        self._objects[key] = S3Object(key, body, self)

    def get(self, key):
        return self._objects.get(key, None)

    def delete(self, key):
        try:
            self._objects.pop(key)
        except KeyError:
            pass


class S3ResourceMock(object):
    def __init__(self):
        self._buckets = {}

    def Bucket(self, name):  # noqa
        if name not in self._buckets:
            self._buckets[name] = S3Bucket(name)
            self._buckets[name].put_object = MagicMock()
        return self._buckets[name]

    def Object(self, name, key):  # noqa
        return self._buckets[name].get(key)


class TestSQSExtendedClient(TestCase):
    SQS_QUEUE_URL = 'test-queueurl'

    LESS_THAN_SQS_SIZE_LIMIT = 3
    SQS_SIZE_LIMIT = 262144
    MORE_THAN_SQS_SIZE_LIMIT = SQS_SIZE_LIMIT + 1
    ARBITRATY_SMALLER_THRESSHOLD = 500

    def setUp(self):
        self.s3_bucket_name = 'unittest-bucket'
        self.sqs_conn_mock = SQSClientMock()
        self.s3_conn_mock = S3ResourceMock()

        def mock_sqs():
            return self.sqs_conn_mock

        def mock_s3():
            return self.s3_conn_mock

        SQSClientExtended.sqs = mock_sqs()
        SQSClientExtended.s3 = mock_s3()

        self.extendedsqs = SQSClientExtended(
            s3_bucket_name=self.s3_bucket_name
        )

        self.extendedsqs.create_queue(
            QueueName=self.SQS_QUEUE_URL
        )

    def test_str_base64_convert(self):
        s = 'test text'
        b64 = self.extendedsqs._str_to_base64(s)
        self.assertEqual(s, self.extendedsqs._base64_to_str(b64))

    def test_is_base64(self):
        s = 'not base64'
        self.assertFalse(self.extendedsqs._is_base64(s))
        self.assertTrue(self.extendedsqs._is_base64(self.extendedsqs._str_to_base64(s).decode()))

    def test_get_string_size_in_byte(self):
        s = 'test message'
        self.assertEqual(self.extendedsqs._get_string_size_in_bytes(s), len(s))

    def test_get_message_attributes_size(self):
        pass

    def test_is_large(self):
        pass

    def test_send_small_payload(self):
        """Send small payload and S3 is not used.
        """
        message_body = self._generate_str(self.SQS_SIZE_LIMIT)
        self.extendedsqs.send_message(
            QueueUrl=self.SQS_QUEUE_URL,
            MessageBody=message_body
        )

        self.extendedsqs.s3.Bucket(self.extendedsqs.s3_bucket_name).put_object.assert_not_called()

    def test_send_large_payload(self):
        """Send large payload and S3 is used.
        """
        message_body = self._generate_str(self.MORE_THAN_SQS_SIZE_LIMIT)
        self.extendedsqs.send_message(
            QueueUrl=self.SQS_QUEUE_URL,
            MessageBody=message_body)

        self.extendedsqs.s3.Bucket(self.extendedsqs.s3_bucket_name).put_object.assert_called_once()

    def test_send_large_payload_disabled(self):
        """Send large payload when large payload support is disabled.
        """
        message_body = self._generate_str(self.MORE_THAN_SQS_SIZE_LIMIT)
        self.extendedsqs.set_large_payload_support(enabled=False)

        with self.assertRaises(ValueError):
            self.extendedsqs.send_message(
                QueueUrl=self.SQS_QUEUE_URL,
                MessageBody=message_body
            )

        self.extendedsqs.s3.Bucket(self.extendedsqs.s3_bucket_name).put_object.assert_not_called()

    def test_send_small_payload_always_through_s3(self):
        """Send small payload if always through s3
        """
        message_body = self._generate_str(self.LESS_THAN_SQS_SIZE_LIMIT)
        self.extendedsqs.set_always_through_s3(always_through_s3=True)

        self.extendedsqs.send_message(
            QueueUrl=self.SQS_QUEUE_URL,
            MessageBody=message_body
        )

        self.extendedsqs.s3.Bucket(self.extendedsqs.s3_bucket_name).put_object.assert_called_once()

    def test_set_message_size_threshold(self):
        message_body = self._generate_str(self.ARBITRATY_SMALLER_THRESSHOLD * 2)
        self.extendedsqs.set_message_size_threshold(self.ARBITRATY_SMALLER_THRESSHOLD)
        self.extendedsqs.send_message(
            QueueUrl=self.SQS_QUEUE_URL,
            MessageBody=message_body
        )

        self.extendedsqs.s3.Bucket(self.extendedsqs.s3_bucket_name).put_object.assert_called_once()

    def test_only_large_payload_to_s3_when_send_message_batch(self):
        pass

    def test_message_attribute_when_send_large_payload(self):
        message_body = self._generate_str(self.MORE_THAN_SQS_SIZE_LIMIT)
        self.extendedsqs.send_message(
            QueueUrl=self.SQS_QUEUE_URL,
            MessageBody=message_body
        )

        msg = self.extendedsqs.sqs.peek_message(
            QueueUrl=self.SQS_QUEUE_URL
        )

    def _generate_str(self, length):
        return 's' * length
