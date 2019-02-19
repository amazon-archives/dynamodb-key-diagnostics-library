/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "LICENSE" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.dynamodb.diagnostics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class DynamoDBKeyDiagnosticsClientAsyncTest {
    @Mock
    AmazonDynamoDBAsync mockDB;

    @Mock
    AmazonKinesis mockKinesis;

    private static final String STREAM_NAME = "stream";
    private static final String TABLE_NAME = "table";
    private static final String KEY_NAME = "key";

    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testPutItem() throws InterruptedException {
        PutItemRequest request = new PutItemRequest()
            .withTableName(TABLE_NAME)
            .withItem(ImmutableMap.of(KEY_NAME, new AttributeValue().withB(ByteBuffer.wrap(new byte[] { 5, 7 }))));
        PutItemResult result = new PutItemResult().withConsumedCapacity(
            new ConsumedCapacity().withCapacityUnits(3.0).withTableName(TABLE_NAME));
        doAnswer(inv -> answerOnSuccess(request, result, inv))
            .when(mockDB).putItemAsync(any(PutItemRequest.class), any());

        AsyncHandler<PutItemRequest, PutItemResult> mockHandler = mock(AsyncHandler.class);
        try (DynamoDBKeyDiagnosticsClientAsync client = createClient()) {
            client.putItemAsync(request, mockHandler);
        }
        assertThat(getLastKinesisInput(), containsString(
            "'IO':3.0,'Operation':'PutItem','Table':'table'," +
                "'KeyValues':[{'name':'key','value':'BQc='}]"));
        verify(mockHandler, times(1)).onSuccess(request, result);
    }

    @Test
    public void testGetItem() throws InterruptedException, ExecutionException {
        GetItemRequest request = new GetItemRequest()
            .withTableName(TABLE_NAME)
            .withKey(ImmutableMap.of(KEY_NAME, new AttributeValue().withS("xx")));
        GetItemResult result = new GetItemResult().withConsumedCapacity(
            new ConsumedCapacity().withCapacityUnits(0.5).withTableName(TABLE_NAME));
        doAnswer(inv -> answerOnSuccess(request, result, inv))
            .when(mockDB).getItemAsync(any(GetItemRequest.class), any());

        AsyncHandler<GetItemRequest, GetItemResult> mockHandler = mock(AsyncHandler.class);
        try (DynamoDBKeyDiagnosticsClientAsync client = createClient()) {
            client.getItemAsync(request, mockHandler);
        }
        assertThat(getLastKinesisInput(), containsString(
            "'IO':0.5,'Operation':'GetItem','Table':'table'," +
                "'KeyValues':[{'name':'key','value':'xx'}]"));
        verify(mockHandler, times(1)).onSuccess(request, result);
    }

    @Test
    public void testBatchGetItem() throws InterruptedException {
        BatchGetItemRequest request = new BatchGetItemRequest()
            .withRequestItems(ImmutableMap.of(TABLE_NAME,
                new KeysAndAttributes().withKeys(
                    ImmutableMap.of(KEY_NAME, new AttributeValue().withS("k1")),
                    ImmutableMap.of(KEY_NAME, new AttributeValue().withS("k2"))
                )
            ));
        BatchGetItemResult result = new BatchGetItemResult().withConsumedCapacity(
            new ConsumedCapacity().withCapacityUnits(5.0).withTableName(TABLE_NAME));
        doAnswer(inv -> answerOnSuccess(request, result, inv))
            .when(mockDB).batchGetItemAsync(Mockito.<BatchGetItemRequest>any(), any());

        AsyncHandler<BatchGetItemRequest, BatchGetItemResult> mockHandler = mock(AsyncHandler.class);
        try (DynamoDBKeyDiagnosticsClientAsync client = createClient()) {
            client.batchGetItemAsync(request, mockHandler);
        }
        final List<String> kinesisInputs = getLastKinesisInputs(2);
        assertThat(kinesisInputs, allOf(
            hasItem(containsString(
                "'IO':2.5,'Operation':'BatchGet','Table':'table'," +
                    "'KeyValues':[{'name':'key','value':'k1'}]")),
            hasItem(containsString(
                "'IO':2.5,'Operation':'BatchGet','Table':'table'," +
                    "'KeyValues':[{'name':'key','value':'k2'}]"))
        ));
        verify(mockHandler, times(1)).onSuccess(request, result);
    }

    @Test
    public void testBatchWriteItem() throws InterruptedException {
        BatchWriteItemRequest request = new BatchWriteItemRequest()
            .withRequestItems(ImmutableMap.of(
                TABLE_NAME, ImmutableList.of(
                    new WriteRequest().withPutRequest(new PutRequest().withItem(ImmutableMap.of(
                        KEY_NAME, new AttributeValue().withS("k1")
                    ))),
                    new WriteRequest().withPutRequest(new PutRequest().withItem(ImmutableMap.of(
                        KEY_NAME, new AttributeValue().withS("k2")
                    ))),
                    new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(ImmutableMap.of(
                        KEY_NAME, new AttributeValue().withS("k3")
                    )))
                )
            ));
        BatchWriteItemResult result = new BatchWriteItemResult().withConsumedCapacity(
            new ConsumedCapacity().withCapacityUnits(6.0).withTableName(TABLE_NAME));
        doAnswer(inv -> answerOnSuccess(request, result, inv))
            .when(mockDB).batchWriteItemAsync(Mockito.<BatchWriteItemRequest>any(), any());

        AsyncHandler<BatchWriteItemRequest, BatchWriteItemResult> mockHandler = mock(AsyncHandler.class);
        try (DynamoDBKeyDiagnosticsClientAsync client = createClient()) {
            client.batchWriteItemAsync(request, mockHandler);
        }
        final List<String> kinesisInputs = getLastKinesisInputs(3);
        assertThat(kinesisInputs, allOf(
            hasItem(containsString(
                "'IO':2.0,'Operation':'BatchWrite.Put','Table':'table'," +
                    "'KeyValues':[{'name':'key','value':'k1'}]")),
            hasItem(containsString(
                "'IO':2.0,'Operation':'BatchWrite.Put','Table':'table'," +
                    "'KeyValues':[{'name':'key','value':'k2'}]")),
            hasItem(containsString(
                "'IO':2.0,'Operation':'BatchWrite.Delete','Table':'table'," +
                    "'KeyValues':[{'name':'key','value':'k3'}]"))
        ));
        verify(mockHandler, times(1)).onSuccess(request, result);
    }

    private DynamoDBKeyDiagnosticsClientAsync createClient() {
        System.setProperty(SDKGlobalConfiguration.AWS_REGION_SYSTEM_PROPERTY, "us-west-2");
        return DynamoDBKeyDiagnosticsClientBuilder.standard(STREAM_NAME)
            .withUnderlyingAsyncClient(mockDB)
            .withKinesisClient(mockKinesis)
            .addKeyToMonitor(TABLE_NAME, KEY_NAME)
            .buildAsync();
    }

    private String getLastKinesisInput() {
        ArgumentCaptor<ByteBuffer> captor = ArgumentCaptor.forClass(ByteBuffer.class);
        verify(mockKinesis).putRecord(eq(STREAM_NAME), captor.capture(), any());
        return toJsonString(captor.getValue());
    }

    private List<String> getLastKinesisInputs(final int expectedNumCalls) {
        ArgumentCaptor<ByteBuffer> captor = ArgumentCaptor.forClass(ByteBuffer.class);
        verify(mockKinesis, times(expectedNumCalls)).putRecord(eq(STREAM_NAME), captor.capture(), any());
        return captor.getAllValues().stream().map(this::toJsonString).collect(Collectors.toList());
    }

    private String toJsonString(ByteBuffer value) {
        return new String(value.array(), StandardCharsets.UTF_8).replace('\"', '\'');
    }

    @SuppressWarnings("unchecked")
    private <Request extends AmazonWebServiceRequest, Result extends AmazonWebServiceResult<ResponseMetadata>> Void answerOnSuccess(
        Request request, Result result, InvocationOnMock mock) {

        AsyncHandler<Request, Result> async = mock.getArgument(1);
        async.onSuccess(request, result);
        return null;
    }
}
