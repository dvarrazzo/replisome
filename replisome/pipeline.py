class Pipeline(object):
    """A chain of operations on a stream of changes"""
    NOT_STARTED = 'NOT_STARTED'
    RUNNING = 'RUNNING'
    STOPPED = 'STOPPED'

    def __init__(self):
        self.receiver = None
        self.filters = []
        self.consumer = None
        self.state = self.NOT_STARTED

    def start(self):
        if self.state != self.NOT_STARTED:
            raise ValueError("can't start pipeline in state %s" % self.state)

        if not self.receiver:
            raise ValueError("can't start: no receiver")

        if not self.consumer:
            raise ValueError("can't start: no consumer")

        self.receiver.message_cb = self.process_message
        cnn = self.receiver.create_connection()

        self.state = self.RUNNING
        self.receiver.start(cnn)

    def stop(self):
        if self.state != self.RUNNING:
            raise ValueError("can't stop pipeline in state %s" % self.state)

        self.receiver.stop()
        self.state = self.STOPPED

    def process_message(self, msg):
        for f in self.filters:
            msg = f(msg)
            if msg is None:
                return

        self.consumer(msg)
