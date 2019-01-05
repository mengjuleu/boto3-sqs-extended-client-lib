class SQSClientExtendedBase(object):
    def send_message(self, **kwargs):
        raise NotImplementedError('send_message is not implemented.')

    def send_message_batch(self, **kwargs):
        raise NotImplementedError('send_message_batch is not implemented')

    def receive_message(self, **kwargs):
        raise NotImplementedError('receive_message is not implemented.')

    def delete_message(self, **kwargs):
        raise NotImplementedError('delete_message is not implemented')

    def delete_message_batch(self, **kwargs):
        raise NotImplementedError('delete_message_batch is not implemented')

    def change_message_visibility(self, **kwargs):
        raise NotImplementedError('change_message_visibility is not implemented')

    def change_message_visibility_batch(self, **kwargs):
        raise NotImplementedError('change_message_visibility_batch is not implemented')

    def purge_queue(self, **kwargs):
        raise NotImplementedError('purge_queue is not implemented')

    def create_queue(self, **kwargs):
        raise NotImplementedError('create_queue is not implemented')
