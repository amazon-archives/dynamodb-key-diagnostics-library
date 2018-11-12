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

from botocore.stub import Stubber, ANY
from diagnostics.hot_key_logger_lambda import lambda_handler, CLOUDWATCH_CLIENT, LOGS_CLIENT,\
    AwsHandler, KinesisRecord
from datetime import datetime
from mock import MagicMock
import base64
import unittest


CLOUDWATCH_METRIC_NAMESPACE = 'DynamoDBKeyDiagnosticsLibrary'
TABLE_NAME = 'test-table'
HASH_KEY = 'movie'
HASH_KEY_VALUE1 = 'awesome_movie1'
HASH_KEY_VALUE2 = 'awesome_movie2'
TIMESTAMP1 = '2018-10-01 05:01:00.000'
TIMESTAMP2 = '2018-10-01 05:02:00.000'


class TestHotKeyLoggerLambda(unittest.TestCase):

    NEXT_TOKEN = 'next_token'
    EVENT_RECORDS = [
        {
            'data': base64.b64encode(str({
                'TotalIO': 12001.0,
                'Minute': TIMESTAMP1,
                'TableName': TABLE_NAME,
                'HashKey': HASH_KEY,
                'HashKeyValue': HASH_KEY_VALUE1
            })),
            'recordId': 'mock_record_id1'
        },
        {
            'data': base64.b64encode(str({
                'TotalIO': 19999.0,
                'Minute': TIMESTAMP2,
                'TableName': TABLE_NAME,
                'HashKey': HASH_KEY,
                'HashKeyValue': HASH_KEY_VALUE2
            })),
            'recordId': 'mock_record_id2'
        }
    ]

    def setUp(self):
        self.cloudwatch_stubber = Stubber(CLOUDWATCH_CLIENT)
        self.logs_stubber = Stubber(LOGS_CLIENT)

        for _ in self.EVENT_RECORDS:
            self.cloudwatch_stubber.add_response('put_metric_data', dict(), expected_params={
                    'Namespace': ANY,
                    'MetricData': ANY,
            })
        self.cloudwatch_stubber.activate()
        self.logs_stubber.activate()
        kinesis_records = [
            KinesisRecord(1.0,
                          datetime.strptime(TIMESTAMP1, '%Y-%m-%d %H:%M:%S.000'),
                          TABLE_NAME,
                          HASH_KEY,
                          HASH_KEY_VALUE1),
            KinesisRecord(1.0,
                          datetime.strptime(TIMESTAMP2, '%Y-%m-%d %H:%M:%S.000'),
                          TABLE_NAME,
                          HASH_KEY,
                          HASH_KEY_VALUE2)
            ]
        AwsHandler.deserialize_kinesis_record = MagicMock(side_effect=kinesis_records)

    def test_lambda_handler_with_normal_event(self):
        event = {
            'records': self.EVENT_RECORDS
        }
        with self.cloudwatch_stubber and self.logs_stubber:
            response = lambda_handler(event, None)
            self.assertEqual(2, len(response['records']), "Response size is different from expected!")
            self.assertDictEqual(AwsHandler.create_kinesis_data_analytics_result(
                self.EVENT_RECORDS[0]['recordId'],
                self.EVENT_RECORDS[0]['data'],
                True
            ), response['records'][0])
            self.assertDictEqual(AwsHandler.create_kinesis_data_analytics_result(
                self.EVENT_RECORDS[1]['recordId'],
                self.EVENT_RECORDS[1]['data'],
                True
            ), response['records'][1])
