import json
import uuid
import base64

from enum import Enum
from boto3.session import Session
from botocore.exceptions import ClientError
from extendedsqsclient.SQSClientExtendedBase import SQSClientExtendedBase


class SQSExtendedClientConstants(Enum):
    DEFAULT_MESSAGE_SIZE_THRESHOLD = 262144
    MAX_ALLOWED_ATTRIBUTES = 10 - 1  # 10 for SQS, 1 for the reserved attribute
    RESERVED_ATTRIBUTE_NAME = "SQSLargePayloadSize"
    S3_BUCKET_NAME_MARKER = "-..s3BucketName..-"
    S3_KEY_MARKER = "-..s3Key..-"


S3_KEY = 's3Key'
S3_BUCKET_NAME = 's3BucketName'


class SQSClientExtended(SQSClientExtendedBase):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: AWS access key ID
    :type aws_secret_access_key: string
    :param aws_secret_access_key: AWS secret access key
    :type aws_session_token: string
    :param aws_session_token: AWS temporary session token
    :type region_name: string
    :param region_name: Default region when creating new connections
    :type botocore_session: botocore.session.Session
    :param botocore_session: Use this Botocore session instead of creating a new default one.
    :type profile_name: string
    :param profile_name: The name of a profile to use. If not given, then the default profile is used.
    """

    _session = None
    _s3 = None
    _sqs = None

    def __init__(self, **kwargs):
        aws_access_key_id = kwargs.get('aws_access_key_id')
        aws_secret_access_key = kwargs.get('aws_secret_access_key')
        aws_region_name = kwargs.get('region_name')
        s3_bucket_name = kwargs.get('s3_bucket_name')
        endpoint_url = kwargs.get('endpoint_url')

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_region_name = aws_region_name
        self.s3_bucket_name = s3_bucket_name
        self.message_size_threshold = SQSExtendedClientConstants.DEFAULT_MESSAGE_SIZE_THRESHOLD.value
        self.always_through_s3 = False
        self.large_payload_supported = True

    def is_large_payload_support_enabled(self):
        return self.large_payload_supported

    def set_large_payload_support(self, enabled):
        self.large_payload_supported = enabled

    def set_always_through_s3(self, always_through_s3):
        """
        Sets whether or not all messages regardless of their payload size should be stored in Amazon S3.
        """
        self.always_through_s3 = always_through_s3

    def set_message_size_threshold(self, message_size_threshold):
        """
        Sets the message size threshold for storing message payloads in Amazon S3.
        Default: 256KB.
        """
        self.message_size_threshold = message_size_threshold

    def _get_string_size_in_bytes(self, message_body):
        return len(message_body.encode('utf-8'))

    def _str_to_base64(self, s):
        """Convert string s to base64 encoded
        :param s: string
        :return: bytes
        """
        return base64.b64encode(s.encode('utf-8'))

    def _base64_to_str(self, b):
        """Convert base64 bytes to string
        :param b: bytes
        :return: string
        """
        return base64.b64decode(b).decode('utf-8')

    def _is_base64(self, s):
        """Check if a string is base64 encoded
        :param s: str
        :return:
        """
        try:
            encoded = self._str_to_base64(self._base64_to_str(s))
            return encoded == str.encode(s)
        except Exception as e:
            return False

    def _get_msg_attributes_size(self, message_attributes):
        # sqs binaryValue expects a base64 encoded string as all messages in sqs are strings
        total_msg_attributes_size = 0

        for key, entry in message_attributes.items():
            total_msg_attributes_size += self._get_string_size_in_bytes(key)
            if entry.get('DataType'):
                total_msg_attributes_size += self._get_string_size_in_bytes(entry.get('DataType'))
            if entry.get('StringValue'):
                total_msg_attributes_size += self._get_string_size_in_bytes(entry.get('StringValue'))
            if entry.get('BinaryValue'):
                if self._is_base64(entry.get('BinaryValue')):
                    total_msg_attributes_size += len(str.encode(entry.get('BinaryValue')))
                else:
                    total_msg_attributes_size += self._get_string_size_in_bytes(entry.get('BinaryValue'))

        return total_msg_attributes_size

    def _is_large(self, message, message_attributes):
        msg_attributes_size = self._get_msg_attributes_size(message_attributes)
        msg_body_size = self._get_string_size_in_bytes(message)
        total_msg_size = msg_attributes_size + msg_body_size
        return total_msg_size > self.message_size_threshold

    def receive_message(self, **kwargs):
        """Retrieves one or more messages (up to 10),
        from the specified queue.
        """
        queue_url = kwargs.get('QueueUrl')

        if queue_url is None:
            raise ValueError('QueueUrl is required')

        max_num_of_message = kwargs.get('MaxNumberOfMessages') or 1
        wait_time_seconds = kwargs.get('WaitTimeSeconds') or 0
        visibility_timeout = kwargs.get('VisibilityTimeout') or 30

        if not self.is_large_payload_support_enabled():
            return self.sqs.receive_message(**kwargs)

        resp = self.sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['All', ],
            MessageAttributeNames=['All', ],
            MaxNumberOfMessages=max_num_of_message,
            VisibilityTimeout=visibility_timeout,
            WaitTimeSeconds=wait_time_seconds
        )

        messages = resp.get('Messages', [])

        if not messages:
            return None

        for message in messages:
            large_pay_load_attribute_value = message.get('MessageAttributes', {})\
                                                    .get(SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME.value, None)

            if large_pay_load_attribute_value:
                try:
                    message_body = json.loads(message.get('Body'))
                except ValueError:
                    raise ValueError('Decoding JSON has failed')
                else:
                    if (S3_BUCKET_NAME not in message_body) or (S3_KEY not in message_body):
                        raise ValueError('Detected missing required key attribute'
                                         's3BucketName and s3Key in s3 payload')

                    s3_bucket_name = message_body.get('s3BucketName')
                    s3_key = message_body.get('s3Key')
                    orig_msg_body = self._get_text_from_s3(s3_bucket_name, s3_key)
                    message['Body'] = orig_msg_body

                    # remove the additional attribute before returning the message to user.
                    message.get('MessageAttributes').pop(
                        SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME.value
                    )

                    # Embed s3 object pointer in the receipt handle.
                    message['ReceiptHandle'] = self._embed_s3_pointer_in_receipt_handle(
                        receipt_handle=message.get('ReceiptHandle'),
                        s3_bucket_name=s3_bucket_name,
                        s3_key=s3_key
                    )

        return messages

    def _delete_message_payload_from_s3(self, receipt_handle):
        try:
            s3_msg_bucket_name = self._get_bucket_marker_from_receipt_handle(
                receipt_handle,
                SQSExtendedClientConstants.S3_BUCKET_NAME_MARKER.value)
            s3_msg_key = self._get_bucket_marker_from_receipt_handle(
                receipt_handle,
                SQSExtendedClientConstants.S3_KEY_MARKER.value)
            s3_object = self.s3.Object(s3_msg_bucket_name, s3_msg_key)
            s3_object.delete()
            print('Deleted s3 object https://s3.amazonaws.com/{}/{}'
                  .format(s3_msg_bucket_name, s3_msg_key))
        except Exception as e:
            print("Failed to delete the message content in S3 object. {}, type:{}"
                  .format(str(e), type(e).__name__))
            raise e

    def _get_bucket_marker_from_receipt_handle(self, receipt_handle, marker):
        start_marker = receipt_handle.index(marker) + len(marker)
        end_marker = receipt_handle.rindex(marker, start_marker)
        return receipt_handle[start_marker:end_marker]

    def _get_orig_receipt_handle(self, receipt_handle):
        return receipt_handle[receipt_handle.rindex(SQSExtendedClientConstants.S3_KEY_MARKER.value)
                              + len(SQSExtendedClientConstants.S3_KEY_MARKER.value):]

    def _is_s3_receipt_handle(self, receipt_handle):
        return SQSExtendedClientConstants.S3_BUCKET_NAME_MARKER.value in receipt_handle \
               and SQSExtendedClientConstants.S3_KEY_MARKER.value in receipt_handle

    def _embed_s3_pointer_in_receipt_handle(self, receipt_handle, s3_bucket_name, s3_key):
        return '{}{}{}{}{}{}{}'.format(
            SQSExtendedClientConstants.S3_BUCKET_NAME_MARKER.value,
            s3_bucket_name,
            SQSExtendedClientConstants.S3_BUCKET_NAME_MARKER.value,
            SQSExtendedClientConstants.S3_KEY_MARKER.value,
            s3_key,
            SQSExtendedClientConstants.S3_KEY_MARKER.value,
            receipt_handle,
        )

    def delete_message(self, **kwargs):
        """Deletes the specified message from the specified queue.
        """

        queue_url = kwargs.get('QueueUrl')
        receipt_handle = kwargs.get('ReceiptHandle')

        if queue_url is None:
            raise ValueError('QueueUrl is required')

        if receipt_handle is None:
            raise ValueError('ReceiptHandle is required')

        if not self.is_large_payload_support_enabled():
            return self.sqs.delete_message(**kwargs)

        if self._is_s3_receipt_handle(receipt_handle):
            self._delete_message_payload_from_s3(receipt_handle)
            receipt_handle = self._get_orig_receipt_handle(receipt_handle)

        self.sqs.delete_message(QueueUrl=queue_url,
                                ReceiptHandle=receipt_handle)

    def delete_message_batch(self, **kwargs):
        pass

    def send_message(self, **kwargs):
        """Delivers a message to the specified queue and
        uploads the message payload to Amazon S3 if necessary.

        :param QueueUrl: str
        :param MessageBody: str
        """

        queue_url = kwargs.get('QueueUrl')
        message_body = kwargs.get('MessageBody')
        message_attributes = kwargs.get('MessageAttributes')

        if queue_url is None:
            raise ValueError('QueueUrl is required')

        if message_body is None:
            raise ValueError('MessageBody is required')

        if message_attributes is None:
            message_attributes = {}

        if not self.is_large_payload_support_enabled():
            return self.sqs.send_message(**kwargs)

        if self.always_through_s3 or self._is_large(message_body, message_attributes):
            self._check_message_attributes(message_attributes)

            if not self.s3_bucket_name.strip():
                raise ValueError('S3 bucket name cannot be null')

            large_payload = message_body
            message_body = json.dumps(self._store_message_in_s3(message_body))
            message_attributes[SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME.value] = {
                'StringValue': str(self._get_string_size_in_bytes(large_payload)),
                'DataType': 'Number'}

        return self.sqs.send_message(QueueUrl=queue_url,
                                     MessageBody=message_body,
                                     MessageAttributes=message_attributes)

    def send_message_batch(self, **kwargs):
        queue_url = kwargs.get('QueueUrl')
        entries = kwargs.get('Entries')

        if queue_url is None:
            raise ValueError('QueueUrl is required')

        if entries is None:
            raise ValueError('Entries is required')

        if not self.is_large_payload_support_enabled():
            return self.sqs(**kwargs)

        for i in range(len(entries)):
            if self.always_through_s3 or self._is_large(entries[i]['MessageBody'],
                                                        entries[i]['MessageAttributes']):
                large_payload = entries[i]['MessageBody']
                self._check_message_attributes(entries[i]['MessageAttributes'])
                entries[i]['MessageBody'] = json.dumps(
                    self._store_message_in_s3(entries[i]['MessageBody']))
                entries[i]['MessageAttributes'][SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME.value] = {
                    'StringValue': str(self._get_string_size_in_bytes(large_payload)),
                    'DataType': 'Number'
                }

        return self.sqs.send_message_batch(
            QueueUrl=queue_url,
            Entries=entries
        )

    def change_message_visibility(self, **kwargs):
        """Change the visibility timeout of message
        """
        queue_url = kwargs.get('QueueUrl')
        receipt_handle = kwargs.get('ReceiptHandle')
        visibility_timeout = kwargs.get('VisibilityTimeout')

        if queue_url is None:
            raise ValueError('QueueUrl is required')

        if receipt_handle is None:
            raise ValueError('ReceiptHandle is required')

        if visibility_timeout is None:
            raise ValueError('VisibilityTimeout is required')

        if self._is_s3_receipt_handle(receipt_handle):
            receipt_handle = self._get_orig_receipt_handle(receipt_handle)

        self.sqs.change_message_visibility(
            QueueUrl=queue_url,
            receiptHandle=receipt_handle,
            VisibilityTimeout=visibility_timeout
        )

    def change_message_visibility_batch(self, **kwargs):
        queue_url = kwargs.get('QueueUrl')
        entries = kwargs.get('Entries')

        if queue_url is None:
            raise ValueError('QueueUrl is required')

        if entries is None:
            raise ValueError('Entries is required')

        self.sqs.change_message_visibility_batch(**kwargs)

    def purge_queue(self, **kwargs):
        queue_url = kwargs.get('QueueUrl')

        if queue_url is None:
            raise ValueError('QueueUrl is required')

        self.sqs.purge_queue(**kwargs)

    def create_queue(self, **kwargs):
        queue_name = kwargs.get('QueueName')

        if queue_name is None:
            raise ValueError('QueueName is required')

        self.sqs.create_queue(**kwargs)

    def _check_message_attributes(self, message_attributes):
        """Check if message attributes is valid or not
        :param message_attributes: dict
        :return: None
        """
        msg_attributes_size = self._get_msg_attributes_size(message_attributes)
        if msg_attributes_size > self.message_size_threshold:
            raise ValueError("Total size of Message attributes is {} bytes "
                             "which is larger than the threshold of {} Bytes. "
                             "Consider including the payload in the message "
                             "body instead of message attributes."
                             .format(msg_attributes_size, self.message_size_threshold))

        message_attributes_num = len(message_attributes)
        if message_attributes_num > SQSExtendedClientConstants.MAX_ALLOWED_ATTRIBUTES.value:
            raise ValueError("Number of message attributes [{}}] exceeds "
                             "the maximum allowed for large-payload messages [{}]."
                             .format(message_attributes_num,
                                     SQSExtendedClientConstants.MAX_ALLOWED_ATTRIBUTES.value))

        large_payload_attribute_value = message_attributes.get(
            SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME.value)
        if large_payload_attribute_value:
            raise ValueError("Message attribute name {} is reserved "
                             "for use by SQS extended client."
                             .format(SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME.value))

    def _store_message_in_s3(self, message_body):
        """Store SQS message body into user defined s3 bucket
        prerequisite aws credentials should have access to
        write to defined s3 bucket
        """
        try:
            s3_key = str(uuid.uuid4())
            self.s3.Bucket(self.s3_bucket_name).put_object(
                Key=s3_key,
                Body=message_body
            )
            return {'s3BucketName': self.s3_bucket_name, 's3Key': s3_key}
        except Exception as e:
            print("Failed to store the message content in an S3 object."
                  "SQS message was not sent. {}, type:{}".
                  format(str(e), type(e).__name__))
            raise e

    def _get_text_from_s3(self, s3_bucket_name, s3_key):
        """Get string representation of a sqs object and
        store into original SQS message object
        """
        try:
            obj = self.s3.Object(s3_bucket_name, s3_key)
        except ClientError:
            return ""
        else:
            return obj.get().get('Body').read()

    @property
    def session(self):
        if self._session is None:
            if self.aws_access_key_id and self.aws_secret_access_key and self.aws_region_name:
                self._session = Session(
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    region_name=self.aws_region_name
                )
            else:
                self._session = Session()
        return self._session

    @property
    def s3(self):
        if self._s3 is None:
            self._s3 = self.session.resource('s3')
        return self._s3

    @property
    def sqs(self):
        if self._sqs is None:
            self._sqs = self.session.client('sqs')
        return self._sqs
