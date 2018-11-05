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
import boto3
import base64
import ast
import os
import uuid


CLOUDWATCH_METRIC_NAMESPACE = 'DynamoDBKeyDiagnosticsLibrary'
CLOUDWATCH_LOGS_GROUP_NAME = os.environ.get('HOT_KEY_LOGS_GROUP_NAME', 'hot-keys-logs-group')
CLOUDWATCH_LOGS_STREAM_PREFIX = os.environ.get('HOT_KEY_LOGS_STREAM_PREFIX', 'hot-keys-logs-stream')
HOT_KEY_MESSAGE_FORMAT = 'Detected the following hot key:\n' \
                         'Table name: {}\n' \
                         'Hash key: {}\n' \
                         'Hash key value: {}\n' \
                         'Total IO: {}\n'

# This defines the IO threshold required
DATA_POINT_EVALUATION_THRESHOLD = 0
# This defines the IO threshold to consider a key as "hot"
HOT_KEY_THRESHOLD = 900

cloudwatch_client = boto3.client('cloudwatch')
logs_client = boto3.client('logs')
epoch = datetime.utcfromtimestamp(0)


def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0


def create_hotkey_metric_data(table_name, hash_key, total_io, timestamp):
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


def create_log_event(timestamp, message):
    return {
        'timestamp': timestamp,
        'message': message
    }


def lambda_handler(event, context):
    # Output results for Kinesis Data Analytics to track if a deliver is successful or not
    output = []
    for record in event['records']:
        output_result = 'Ok'
        payload = ast.literal_eval(base64.b64decode(record['data']))
        print(payload)
        total_io = payload['TotalIO']
        timestamp = datetime.strptime(payload['Minute'], '%Y-%m-%d %H:%M:%S.000')
        if total_io > DATA_POINT_EVALUATION_THRESHOLD:
            try:
                response = cloudwatch_client.put_metric_data(
                    Namespace=CLOUDWATCH_METRIC_NAMESPACE,
                    MetricData=[create_hotkey_metric_data(payload['TableName'], payload['HashKey'], total_io, timestamp)]
                )
                print("PutMetricData response: {}".format(response))
            except Exception as e:
                output_result = 'DeliveryFailed'
                print("Exception caught while putting CloudWatch metrics: {}".format(e))
        if total_io > HOT_KEY_THRESHOLD:
            log_stream_name = "{}-{}".format(CLOUDWATCH_LOGS_STREAM_PREFIX, uuid.uuid4().hex)
            # Create a new log stream so we don't have to keep track of the next sequence token
            try:
                message = HOT_KEY_MESSAGE_FORMAT.format(payload['TableName'], payload['HashKey'], payload['HashKeyValue'], total_io)
                logs_client.create_log_stream(
                    logGroupName=CLOUDWATCH_LOGS_GROUP_NAME,
                    logStreamName=log_stream_name
                )
            except Exception as e:
                output_result = 'DeliveryFailed'
                print("Exception caught while creating CloudWatch Logs Stream: {}".format(e))

            try:
                response = logs_client.put_log_events(
                    logGroupName=CLOUDWATCH_LOGS_GROUP_NAME,
                    logStreamName=log_stream_name,
                    logEvents=[create_log_event(long(unix_time_millis(timestamp)), message)],
                )
                print("PutLogEvents response: {}".format(response))
            except Exception as e:
                output_result = 'DeliveryFailed'
                print("Exception caught while putting CloudWatch Logs: {}".format(e))

        output_record = {
            'recordId': record['recordId'],
            'result': output_result,
            'data': record['data']
        }
        output.append(output_record)

    return {'records': output}

