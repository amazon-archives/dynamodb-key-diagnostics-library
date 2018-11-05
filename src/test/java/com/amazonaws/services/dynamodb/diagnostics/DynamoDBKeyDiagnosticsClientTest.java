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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DynamoDBKeyDiagnosticsClientTest {
    @Mock
    AmazonDynamoDBClient mockDB;

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
    public void testDeleteItem() throws InterruptedException {
        DeleteItemResult result = new DeleteItemResult().withConsumedCapacity(
                new ConsumedCapacity().withCapacityUnits(2.0).withTableName(TABLE_NAME));
        when(mockDB.deleteItem(any())).thenReturn(result);
        try (DynamoDBKeyDiagnosticsClient client = createClient()) {
            client.deleteItem(TABLE_NAME,
                    ImmutableMap.of(KEY_NAME, new AttributeValue().withN("6"))
            );
        }
        assertThat(getLastKinesisInput(), containsString(
                "'IO':2.0,'Operation':'DeleteItem','Table':'table'," +
                        "'KeyValues':[{'name':'key','value':'6'}]"));
    }

    @Test
    public void testPutItem() throws InterruptedException {
        PutItemResult result = new PutItemResult().withConsumedCapacity(
                new ConsumedCapacity().withCapacityUnits(3.0).withTableName(TABLE_NAME));
        when(mockDB.putItem(any())).thenReturn(result);
        try (DynamoDBKeyDiagnosticsClient client = createClient()) {
            client.putItem(TABLE_NAME,
                    ImmutableMap.of(KEY_NAME, new AttributeValue().withB(ByteBuffer.wrap(new byte[] { 5, 7 })))
            );
        }
        assertThat(getLastKinesisInput(), containsString(
                "'IO':3.0,'Operation':'PutItem','Table':'table'," +
                        "'KeyValues':[{'name':'key','value':'BQc='}]"));
    }

    @Test
    public void testGetItem() throws InterruptedException {
        GetItemResult result = new GetItemResult().withConsumedCapacity(
                new ConsumedCapacity().withCapacityUnits(0.5).withTableName(TABLE_NAME));
        when(mockDB.getItem(any())).thenReturn(result);
        try (DynamoDBKeyDiagnosticsClient client = createClient()) {
            client.getItem(TABLE_NAME,
                    ImmutableMap.of(KEY_NAME, new AttributeValue().withS("xx"))
            );
        }
        assertThat(getLastKinesisInput(), containsString(
                "'IO':0.5,'Operation':'GetItem','Table':'table'," +
                        "'KeyValues':[{'name':'key','value':'xx'}]"));
    }

    @Test
    public void testUpdateItem() throws InterruptedException {
        UpdateItemResult result = new UpdateItemResult().withConsumedCapacity(
                new ConsumedCapacity().withCapacityUnits(5.0).withTableName(TABLE_NAME));
        when(mockDB.updateItem(any())).thenReturn(result);
        try (DynamoDBKeyDiagnosticsClient client = createClient()) {
            client.updateItem(TABLE_NAME,
                    ImmutableMap.of(KEY_NAME, new AttributeValue().withS("xxy")),
                    ImmutableMap.of("other-attribute", new AttributeValueUpdate())
            );
        }
        assertThat(getLastKinesisInput(), containsString(
                "'IO':5.0,'Operation':'UpdateItem','Table':'table'," +
                        "'KeyValues':[{'name':'key','value':'xxy'}]"));
    }

    @Test
    public void testBatchGetItem() throws InterruptedException {
        BatchGetItemResult result = new BatchGetItemResult().withConsumedCapacity(
                new ConsumedCapacity().withCapacityUnits(5.0).withTableName(TABLE_NAME));
        when(mockDB.batchGetItem(Mockito.<BatchGetItemRequest>any())).thenReturn(result);
        try (DynamoDBKeyDiagnosticsClient client = createClient()) {
            client.batchGetItem(ImmutableMap.of(TABLE_NAME,
                    new KeysAndAttributes().withKeys(
                            ImmutableMap.of(KEY_NAME, new AttributeValue().withS("k1")),
                            ImmutableMap.of(KEY_NAME, new AttributeValue().withS("k2"))
                    )
            ));
        }
        final List<String> kinesisInputs = getLastKinesisInputs(2);
        assertThat(kinesisInputs.get(0), containsString(
                "'IO':2.5,'Operation':'BatchGet','Table':'table'," +
                        "'KeyValues':[{'name':'key','value':'k1'}]"));
        assertThat(kinesisInputs.get(1), containsString(
                "'IO':2.5,'Operation':'BatchGet','Table':'table'," +
                        "'KeyValues':[{'name':'key','value':'k2'}]"));
    }

    @Test
    public void testBatchWriteItem() throws InterruptedException {
        BatchWriteItemResult result = new BatchWriteItemResult().withConsumedCapacity(
                new ConsumedCapacity().withCapacityUnits(6.0).withTableName(TABLE_NAME));
        when(mockDB.batchWriteItem(Mockito.<BatchWriteItemRequest>any())).thenReturn(result);
        try (DynamoDBKeyDiagnosticsClient client = createClient()) {
            client.batchWriteItem(ImmutableMap.of(
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
        }
        final List<String> kinesisInputs = getLastKinesisInputs(3);
        assertThat(kinesisInputs.get(0), containsString(
                "'IO':2.0,'Operation':'BatchWrite.Put','Table':'table'," +
                        "'KeyValues':[{'name':'key','value':'k1'}]"));
        assertThat(kinesisInputs.get(1), containsString(
                "'IO':2.0,'Operation':'BatchWrite.Put','Table':'table'," +
                        "'KeyValues':[{'name':'key','value':'k2'}]"));
        assertThat(kinesisInputs.get(2), containsString(
                "'IO':2.0,'Operation':'BatchWrite.Delete','Table':'table'," +
                        "'KeyValues':[{'name':'key','value':'k3'}]"));
    }

    @Test
    public void testQuery() throws InterruptedException {
        QueryResult result = new QueryResult().withConsumedCapacity(
                new ConsumedCapacity().withCapacityUnits(2.5).withTableName(TABLE_NAME));
        when(mockDB.query(any())).thenReturn(result);
        try (DynamoDBKeyDiagnosticsClient client = createClient()) {
            client.query(new QueryRequest().withTableName(TABLE_NAME)
                    .withKeyConditionExpression(KEY_NAME + " = :v")
                    .withExpressionAttributeValues(ImmutableMap.of(":v", new AttributeValue().withN("5")))
            );
        }
        assertThat(getLastKinesisInput(), containsString(
                "'IO':2.5,'Operation':'Query','Table':'table'," +
                        "'KeyValues':[{'name':'key','value':'5'}]"));
    }

    private DynamoDBKeyDiagnosticsClient createClient() {
        return DynamoDBKeyDiagnosticsClientBuilder.standard(STREAM_NAME)
                .withUnderlyingClient(mockDB)
                .withKinesisClient(mockKinesis)
                .addKeyToMonitor(TABLE_NAME, KEY_NAME)
                .build();
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
}
