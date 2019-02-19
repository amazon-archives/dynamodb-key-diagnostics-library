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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.experimental.Delegate;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A wrapper over a regular DynamoDB client that captures key usage information for every data
 * API call and asynchronously sends that information to a Kinesis stream that can be used to
 * monitor for hot keys.
 *
 * @author Mostafa Elhemali
 * @author Ryan Chan
 */
@SuppressFBWarnings(value = "RI_REDUNDANT_INTERFACES")
public class DynamoDBKeyDiagnosticsClient extends AmazonDynamoDBClient
        implements AmazonDynamoDB, AutoCloseable {

    @Delegate(types = AmazonDynamoDB.class, excludes = Implemented.class)
    private final AmazonDynamoDB underlyingClient;
    private final KinesisStreamReporter streamReporter;

    @Override
    public void close() throws InterruptedException {
        streamReporter.close();
    }

    /**
     * Since all public methods that are part of AmazonDynamoDB are copied and the following methods are implemented
     * in DynamoDBKeyDiagnosticsClient, we would have to exclude them to avoid duplicate method errors.
     */
    private interface Implemented {
        DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest);

        DeleteItemResult deleteItem(String tableName, Map<String, AttributeValue> key);

        DeleteItemResult deleteItem(String tableName, Map<String, AttributeValue> key, String returnValues);

        PutItemResult putItem(PutItemRequest request);

        PutItemResult putItem(String tableName, Map<String, AttributeValue> item);

        PutItemResult putItem(String tableName, Map<String, AttributeValue> item, String returnValues);

        GetItemResult getItem(GetItemRequest request);

        GetItemResult getItem(String tableName, Map<String, AttributeValue> key);

        GetItemResult getItem(String tableName, Map<String, AttributeValue> key, Boolean consistentRead);

        BatchWriteItemResult batchWriteItem(BatchWriteItemRequest batchWriteItemRequest);

        BatchWriteItemResult batchWriteItem(Map<String, List<WriteRequest>> requestItems);

        BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest);

        BatchGetItemResult batchGetItem(Map<String, KeysAndAttributes> requestItems);

        BatchGetItemResult batchGetItem(Map<String, KeysAndAttributes> requestItems, String returnConsumedCapacity);

        QueryResult query(QueryRequest queryRequest);

        UpdateItemResult updateItem(UpdateItemRequest updateItemRequest);

        UpdateItemResult updateItem(String tableName,
                                    Map<String, AttributeValue> key,
                                    Map<String, AttributeValueUpdate> attributeUpdates);

        UpdateItemResult updateItem(String tableName,
                                    Map<String, AttributeValue> key,
                                    Map<String, AttributeValueUpdate> attributeUpdates,
                                    String returnValues);

        void setEndpoint(String var1);
    }

    /**
     * Constructs the wrapper client.
     *
     * @param underlyingClient The regular client we're wrapping.
     * @param streamReporter   The kinesis stream reporter that will publish the key usage information.
     */
    DynamoDBKeyDiagnosticsClient(
            final AmazonDynamoDB underlyingClient,
            final KinesisStreamReporter streamReporter) {
        this.underlyingClient = checkNotNull(underlyingClient);
        this.streamReporter = checkNotNull(streamReporter);
    }

    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public DeleteItemResult deleteItem(final DeleteItemRequest request) {
        final Instant startTime = Instant.now();
        final DeleteItemRequest instrumented = request.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        final DeleteItemResult result = underlyingClient.deleteItem(instrumented);
        final Instant endTime = Instant.now();
        streamReporter.putIntoStream(request, result, startTime, endTime);
        return result;
    }

    public DeleteItemResult deleteItem(final String tableName, final Map<String, AttributeValue> key) {
        return deleteItem(new DeleteItemRequest().withTableName(tableName).withKey(key));
    }

    public DeleteItemResult deleteItem(final String tableName,
                                       final Map<String, AttributeValue> key,
                                       final String returnValues) {
        return deleteItem(new DeleteItemRequest().withTableName(tableName).withKey(key).withReturnValues(returnValues));
    }

    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public PutItemResult putItem(final PutItemRequest request) {
        final Instant startTime = Instant.now();
        final PutItemRequest instrumented = request.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        final PutItemResult result = underlyingClient.putItem(instrumented);
        final Instant endTime = Instant.now();
        streamReporter.putIntoStream(request, result, startTime, endTime);
        return result;
    }

    public PutItemResult putItem(final String tableName, final Map<String, AttributeValue> item) {
        return putItem(new PutItemRequest().withTableName(tableName).withItem(item));
    }

    public PutItemResult putItem(final String tableName,
                                 final Map<String, AttributeValue> item,
                                 final String returnValues) {
        return putItem(new PutItemRequest().withTableName(tableName).withItem(item).withReturnValues(returnValues));
    }

    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public GetItemResult getItem(final GetItemRequest request) {
        final Instant startTime = Instant.now();
        final GetItemRequest instrumented = request.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        final GetItemResult result = underlyingClient.getItem(instrumented);
        final Instant endTime = Instant.now();
        streamReporter.putIntoStream(request, result, startTime, endTime);
        return result;
    }

    public GetItemResult getItem(final String tableName, final Map<String, AttributeValue> key) {
        return getItem(new GetItemRequest().withTableName(tableName).withKey(key));
    }

    public GetItemResult getItem(final String tableName,
                                 final Map<String, AttributeValue> key,
                                 final Boolean consistentRead) {
        return getItem(new GetItemRequest().withTableName(tableName).withKey(key).withConsistentRead(consistentRead));
    }

    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public QueryResult query(final QueryRequest request) {
        final Instant startTime = Instant.now();
        final QueryRequest instrumented = request.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        final QueryResult result = underlyingClient.query(instrumented);
        final Instant endTime = Instant.now();
        streamReporter.putIntoStream(request, result, startTime, endTime);
        return result;
    }

    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public BatchWriteItemResult batchWriteItem(final BatchWriteItemRequest request) {
        final Instant startTime = Instant.now();
        final BatchWriteItemRequest instrumented = request.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        final BatchWriteItemResult result = underlyingClient.batchWriteItem(instrumented);
        final Instant endTime = Instant.now();
        streamReporter.putIntoStream(request, result, startTime, endTime);
        return result;
    }

    public BatchWriteItemResult batchWriteItem(final Map<String, List<WriteRequest>> requestItems) {
        return batchWriteItem(new BatchWriteItemRequest().withRequestItems(requestItems));
    }

    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public BatchGetItemResult batchGetItem(final BatchGetItemRequest request) {
        final Instant startTime = Instant.now();
        final BatchGetItemRequest instrumented = request.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        final BatchGetItemResult result = underlyingClient.batchGetItem(instrumented);
        final Instant endTime = Instant.now();
        streamReporter.putIntoStream(request, result, startTime, endTime);
        return result;
    }

    public BatchGetItemResult batchGetItem(final Map<String, KeysAndAttributes> requestItems) {
        return batchGetItem(new BatchGetItemRequest().withRequestItems(requestItems));
    }

    public BatchGetItemResult batchGetItem(final Map<String, KeysAndAttributes> requestItems,
                                           final String returnConsumedCapacity) {
        return batchGetItem(requestItems);
    }

    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public UpdateItemResult updateItem(final UpdateItemRequest request) {
        final Instant startTime = Instant.now();
        final UpdateItemRequest instrumented = request.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        final UpdateItemResult result = underlyingClient.updateItem(instrumented);
        final Instant endTime = Instant.now();
        streamReporter.putIntoStream(request, result, startTime, endTime);
        return result;
    }

    public UpdateItemResult updateItem(final String tableName,
                                       final Map<String, AttributeValue> key,
                                       final Map<String, AttributeValueUpdate> attributeUpdates) {
        return updateItem(new UpdateItemRequest()
                .withTableName(tableName)
                .withKey(key)
                .withAttributeUpdates(attributeUpdates)
        );
    }

    public UpdateItemResult updateItem(final String tableName,
                                       final Map<String, AttributeValue> key,
                                       final Map<String, AttributeValueUpdate> attributeUpdates,
                                       final String returnValues) {
        return updateItem(new UpdateItemRequest()
                .withTableName(tableName)
                .withKey(key)
                .withAttributeUpdates(attributeUpdates)
                .withReturnValues(returnValues)
        );
    }
}
