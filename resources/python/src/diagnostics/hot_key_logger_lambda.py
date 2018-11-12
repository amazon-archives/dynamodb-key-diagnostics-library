#
# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# or in the "LICENSE" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
#

from __future__ import print_function
from datetime import datetime
import ast
import base64
import boto3
import os
import uuid

# This defines the IO threshold required
DATA_POINT_EVALUATION_THRESHOLD = 0
# This defines the IO threshold to consider a key as "hot"
HOT_KEY_THRESHOLD = 900

CLOUDWATCH_METRIC_NAMESPACE = 'DynamoDBKeyDiagnosticsLibrary'
CLOUDWATCH_LOGS_GROUP_NAME = os.environ.get('HOT_KEY_LOGS_GROUP_NAME', 'hot-keys-logs-group')
CLOUDWATCH_LOGS_STREAM_PREFIX = os.environ.get('HOT_KEY_LOGS_STREAM_PREFIX', 'hot-keys-logs-stream')
CLOUDWATCH_CLIENT = boto3.client('cloudwatch')
LOGS_CLIENT = boto3.client('logs')
HOT_KEY_MESSAGE_FORMAT = 'Detected the following hot key:\n' \
                         'Table name: {}\n' \
                         'Hash key: {}\n' \
                         'Hash key value: {}\n' \
                         'Total IO: {}\n'


def lambda_handler(event, context):
    """
    Main method is triggered by AWS Lambda.
    """

    # Output results for Kinesis Data Analytics to track if a deliver is successful or not
    output = []
    aws_handler = AwsHandler(CLOUDWATCH_CLIENT, LOGS_CLIENT)
    for record in event['records']:
        success = True
        kinesis_record = aws_handler.deserialize_kinesis_record(record)
        if kinesis_record.total_io > DATA_POINT_EVALUATION_THRESHOLD:
            success = aws_handler.record_key_metric(kinesis_record)

        if kinesis_record.total_io > HOT_KEY_THRESHOLD:
            success = aws_handler.record_key_log(kinesis_record)

        output.append(
            aws_handler.create_kinesis_data_analytics_result(
                record['recordId'],
                record['data'],
                success)
        )

    return {'records': output}


class AwsHandler(object):
    """
    Class to handle interactions with AWS services such as CloudWatch Metrics and CloudWatch Logs.
    """

    def __init__(self, cloudwatch_client, logs_client):
        self.cloudwatch_client = cloudwatch_client
        self.logs_client = logs_client

    def record_key_metric(self, kinesis_record):
        """
        This method takes a KinesisRecord object and records it to CloudWatch metrics.
        :return: Boolean, indicating if CloudWatch:PutMetricData was successful or not.
        """
        try:
            response = self.cloudwatch_client.put_metric_data(
                Namespace=CLOUDWATCH_METRIC_NAMESPACE,
                MetricData=[self._create_key_metric_data(
                    kinesis_record.table_name,
                    kinesis_record.hash_key,
                    kinesis_record.total_io,
                    kinesis_record.timestamp)]
            )
            print("PutMetricData response: {}".format(response))
        except Exception as e:
            print("Exception caught while putting CloudWatch metrics: {}".format(e))
            return False
        return True

    def record_key_log(self, kinesis_record):
        """
        This method takes a KinesisRecord object and records it to CloudWatch logs.
        :param kinesis_record:
        :return: Boolean. True if the log stream and logs were successfully created, False otherwise.
        """
        log_stream_name = "{}-{}".format(CLOUDWATCH_LOGS_STREAM_PREFIX, uuid.uuid4().hex)
        # Create a new log stream so we don't have to keep track of the next sequence token
        try:
            message = HOT_KEY_MESSAGE_FORMAT.format(kinesis_record.table_name,
                                                    kinesis_record.hash_key,
                                                    kinesis_record.hash_key_value,
                                                    kinesis_record.total_io)
            self.logs_client.create_log_stream(
                logGroupName=CLOUDWATCH_LOGS_GROUP_NAME,
                logStreamName=log_stream_name
            )
        except Exception as e:
            print("Exception caught while creating CloudWatch Logs Stream: {}".format(e))
            return False

        try:
            response = self.logs_client.put_log_events(
                logGroupName=CLOUDWATCH_LOGS_GROUP_NAME,
                logStreamName=log_stream_name,
                logEvents=[self._create_log_event(long(self._unix_time_millis(kinesis_record.timestamp)), message)],
            )
            print("PutLogEvents response: {}".format(response))
        except Exception as e:
            print("Exception caught while putting CloudWatch Logs: {}".format(e))
            return False
        return True

    @staticmethod
    def deserialize_kinesis_record(record):
        payload = ast.literal_eval(base64.b64decode(record['data']))
        print(payload)
        return KinesisRecord(
            payload['TotalIO'],
            datetime.strptime(payload['Minute'], '%Y-%m-%d %H:%M:%S.000'),
            payload['TableName'],
            payload['HashKey'],
            payload['HashKeyValue']
        )

    @staticmethod
    def create_kinesis_data_analytics_result(record_id, data, success):
        return {
            'recordId': record_id,
            'data': data,
            'result': 'Ok' if success else 'DeliveryFailed'
        }

    @staticmethod
    def _create_key_metric_data(table_name, hash_key, total_io, timestamp):
        return {
            'MetricName': 'Breaching key total IO',
            'Dimensions': [
                {
                    'Name': 'Table name',
                    'Value': table_name
                },
                {
                    'Name': 'Key',
                    'Value': hash_key
                }
            ],
            'Timestamp': timestamp,
            'Value': total_io
        }

    @staticmethod
    def _create_log_event(timestamp, message):
        return {
            'timestamp': timestamp,
            'message': message
        }

    @staticmethod
    def _unix_time_millis(dt):
        epoch = datetime.utcfromtimestamp(0)
        return (dt - epoch).total_seconds() * 1000.0


class KinesisRecord(object):
    def __init__(self, total_io, timestamp, table_name, hash_key, hash_key_value):
        self.total_io = total_io
        self.timestamp = timestamp
        self.table_name = table_name
        self.hash_key = hash_key
        self.hash_key_value = hash_key_value

