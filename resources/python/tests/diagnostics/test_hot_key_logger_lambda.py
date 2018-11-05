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
from diagnostics.hot_key_logger_lambda import lambda_handler, cloudwatch_client, logs_client, create_hotkey_metric_data, create_log_event, CLOUDWATCH_LOGS_GROUP_NAME, \
    HOT_KEY_MESSAGE_FORMAT,\
    CLOUDWATCH_METRIC_NAMESPACE
from botocore.stub import Stubber, ANY
import ast
import base64
import unittest


class TestHotKeyLoggerLambda(unittest.TestCase):

    cloudwatch_stubber = Stubber(cloudwatch_client)
    logs_stubber = Stubber(logs_client)

    NEXT_TOKEN = 'next_token'
    TABLE_NAME = 'test-table'
    HASH_KEY = 'movie'
    EVENT_RECORDS = [
        {
            'data': base64.b64encode(str({
                'TotalIO': 12001.0,
                'Minute': '2018-10-01 05:01:00.000',
                'TableName': TABLE_NAME,
                'HashKey': HASH_KEY,
                'HashKeyValue': 'awesome_movie1'
            })),
            'recordId': 'mock_record_id1'
        },
        {
            'data': base64.b64encode(str({
                'TotalIO': 19999.0,
                'Minute': '2018-10-01 05:02:00.000',
                'TableName': TABLE_NAME,
                'HashKey': HASH_KEY,
                'HashKeyValue': 'awesome_movie2'
            })),
            'recordId': 'mock_record_id2'
        }
    ]

    def setUp(self):
        datapoints = [ast.literal_eval(base64.b64decode(record['data'])) for record in self.EVENT_RECORDS]
        cloudwatch_response = {
            'ResponseMetadata':
                {
                    'RetryAttempts': 0,
                    'HTTPStatusCode': 200,
                    'RequestId': '31332d89-c8c5-11e8-9753-63f440441873',
                    'HTTPHeaders':
                        {
                            'x-amzn-requestid': '31332d89-c8c5-11e8-9753-63f440441873',
                            'date': 'Fri, 05 Oct 2018 17:36:28 GMT',
                            'content-length': '212',
                            'content-type': 'text/xml'
                        }
                }
        }
        for datapoint in datapoints:
            self.cloudwatch_stubber.add_response('put_metric_data', cloudwatch_response, expected_params=
            {
                'Namespace': CLOUDWATCH_METRIC_NAMESPACE,
                'MetricData': [create_hotkey_metric_data(datapoint['TableName'], datapoint['HashKey'], datapoint['TotalIO'], ANY)],
            })
        self.cloudwatch_stubber.activate()

        logs_response = {
            'ResponseMetadata':
                {
                    'RetryAttempts': 0,
                    'HTTPStatusCode': 200,
                    'RequestId': '31332d89-c8c5-11e8-9753-63f440441873',
                    'HTTPHeaders':
                        {
                            'x-amzn-requestid': '31332d89-c8c5-11e8-9753-63f440441873',
                            'date': 'Fri, 05 Oct 2018 17:36:28 GMT',
                            'content-length': '212',
                            'content-type': 'text/xml'
                        }
                }
        }

        self.logs_stubber.add_response('put_log_events', logs_response, expected_params=
        {
            'logGroupName': CLOUDWATCH_LOGS_GROUP_NAME,
            'logStreamName': ANY,
            'logEvents': [create_log_event(ANY, HOT_KEY_MESSAGE_FORMAT.format(datapoints[0]['TableName'], datapoints[0]['HashKey'], datapoints[0]['HashKeyValue'],
                                                                              datapoints[0]['TotalIO']))],
        })
        self.logs_stubber.add_response('put_log_events', logs_response, expected_params=
        {
            'logGroupName': CLOUDWATCH_LOGS_GROUP_NAME,
            'logStreamName': ANY,
            'logEvents': [create_log_event(ANY, HOT_KEY_MESSAGE_FORMAT.format(datapoints[1]['TableName'], datapoints[1]['HashKey'], datapoints[1]['HashKeyValue'],
                                                                              datapoints[1]['TotalIO']))],
        })
        self.logs_stubber.activate()

    def test_lambda_handler_with_normal_event(self):
        event = {
            'records': self.EVENT_RECORDS
        }
        with self.cloudwatch_stubber and self.logs_stubber:
            lambda_handler(event, None)
            self.cloudwatch_stubber.assert_no_pending_responses()
            self.logs_stubber.assert_no_pending_responses()
