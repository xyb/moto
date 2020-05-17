from __future__ import unicode_literals
import unittest
import sure

import boto3
from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message
from boto.ec2 import EC2Connection

from moto import mock_sqs, mock_ec2


class TestNestedDecorators(unittest.TestCase):
    @mock_sqs
    def setup_sqs_queue(self):
        conn = boto3.client("sqs", region_name="us-west-1")
        sqs = boto3.resource("sqs", region_name="us-west-1")
        queue = sqs.create_queue(QueueName="some-queue")
        msg = queue.send_message(MessageBody="This is my first message.")

        int(
            conn.get_queue_attributes(
                QueueUrl=queue.url, AttributeNames=["ApproximateNumberOfMessages"]
            )["Attributes"]["ApproximateNumberOfMessages"]
        ).should.equal(1)

    @mock_ec2
    def test_nested(self):
        self.setup_sqs_queue()

        conn = boto3.client("ec2", region_name="us-west-1")
        conn.run_instances(ImageId="ami-123456", MaxCount=1, MinCount=1)
