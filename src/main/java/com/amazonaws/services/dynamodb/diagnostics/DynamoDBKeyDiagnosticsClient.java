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
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
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
import com.amazonaws.services.kinesis.AmazonKinesis;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A wrapper over a regular DynamoDB client that captures key usage information for every data
 * API call and asynchronously sends that information to a Kinesis stream that can be used to
 * monitor for hot keys.
 *
 * @author Mostafa Elhemali
 * @author Ryan Chan
 */
@Slf4j
@SuppressFBWarnings(value = "RI_REDUNDANT_INTERFACES")
public class DynamoDBKeyDiagnosticsClient extends AmazonDynamoDBClient
        implements AmazonDynamoDB, AutoCloseable {
    private static final Pattern KEY_EXPRESSION_PATTERN = Pattern.compile(
            "(?:^| )(?<Key>[^= ]+) *= *(?<Value>:[^ ]+)(?:$| )"
    );
    private static final int TASK_EXECUTION_TIMEOUT_SECONDS = 60;

    /**
     * Default consumed capacity units for different DynamoDB operations.
     * These defaults are used when the return object from the DynamoDB client does not contain
     * the consumed capacity units.
    */
    static final double DEFAULT_DELETE_CONSUMED_CAPACITY_UNITS = 1.0;
    static final double DEFAULT_PUT_CONSUMED_CAPACITY_UNITS = 1.0;
    static final double DEFAULT_UPDATE_CONSUMED_CAPACITY_UNITS = 1.0;
    static final double DEFAULT_GET_CONSUMED_CAPACITY_UNITS = 0.5;
    static final double DEFAULT_QUERY_CONSUMED_CAPACITY_UNITS = 0.5;

    @Delegate(types = AmazonDynamoDB.class, excludes = Implemented.class)
    private final AmazonDynamoDB underlyingClient;
    private final AmazonKinesis kinesisClient;
    private final String streamName;
    private final ImmutableMap<String, ImmutableSet<String>> tablesAndAttributesToMonitor;
    private final Gson gson = new Gson();
    private final ExecutorService streamPutterService;
    private final boolean useDefaultConsumedCapacity;

    @Override
    public void close() throws InterruptedException {
        streamPutterService.shutdown();
        streamPutterService.awaitTermination(TASK_EXECUTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
     * @param underlyingClient The regular client we're wrapping.
     * @param kinesisClient The Kinesis client we'll use to publish the key usage information.
     * @param streamName The Kinesis stream we'll publish to.
     * @param tablesAndAttributesToMonitor The collection of tables and attributes within those tables that we should
     *                                     monitor for key usage information.
     * @param streamPutterService The executor service to asynchronously log into Kinesis.
     * @param useDefaultConsumedCapacity If true, the DynamoDBKeyDiagnosticsClient will assume each DynamoDB operation
     *                                   consumed a default capacity, even when the response object does not include it
     *                                   (eg. integration testing with DynamoDB Local)
     */
    DynamoDBKeyDiagnosticsClient(
            final AmazonDynamoDB underlyingClient,
            final AmazonKinesis kinesisClient,
            final String streamName,
            final ImmutableMap<String, ImmutableSet<String>> tablesAndAttributesToMonitor,
            final ExecutorService streamPutterService,
            final boolean useDefaultConsumedCapacity) {
        this.underlyingClient = checkNotNull(underlyingClient);
        this.kinesisClient = checkNotNull(kinesisClient);
        this.streamName = checkNotNull(streamName);
        this.tablesAndAttributesToMonitor = checkNotNull(tablesAndAttributesToMonitor);
        this.streamPutterService = streamPutterService;
        this.useDefaultConsumedCapacity = useDefaultConsumedCapacity;
    }

    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public DeleteItemResult deleteItem(final DeleteItemRequest request) {
        final Instant startTime = Instant.now();
        final DeleteItemRequest instrumented = request.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        final DeleteItemResult result = underlyingClient.deleteItem(instrumented);
        final Instant endTime = Instant.now();
        streamPutterService.submit(() -> putIntoStream(request, result, startTime, endTime));
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
        streamPutterService.submit(() -> putIntoStream(request, result, startTime, endTime));
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
        streamPutterService.submit(() -> putIntoStream(request, result, startTime, endTime));
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
        streamPutterService.submit(() -> putIntoStream(request, result, startTime, endTime));
        return result;
    }

    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public BatchWriteItemResult batchWriteItem(final BatchWriteItemRequest request) {
        final Instant startTime = Instant.now();
        final BatchWriteItemRequest instrumented = request.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        final BatchWriteItemResult result = underlyingClient.batchWriteItem(instrumented);
        final Instant endTime = Instant.now();
        streamPutterService.submit(() -> putIntoStream(request, result, startTime, endTime));
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
        streamPutterService.submit(() -> putIntoStream(request, result, startTime, endTime));
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
        streamPutterService.submit(() -> putIntoStream(request, result, startTime, endTime));
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

    private void putIntoStream(final DeleteItemRequest request,
                               final DeleteItemResult result,
                               final Instant startTime,
                               final Instant endTime) {
        final Double consumedCapacityUnits = getConsumedCapacityUnits(result.getConsumedCapacity(),
                DEFAULT_DELETE_CONSUMED_CAPACITY_UNITS);
        putIntoStream(
                request.getTableName(),
                "DeleteItem",
                consumedCapacityUnits.doubleValue(),
                request.getKey()::get,
                startTime,
                endTime
        );
    }

    private void putIntoStream(final PutItemRequest request,
                               final PutItemResult result,
                               final Instant startTime,
                               final Instant endTime) {
        final Double consumedCapacityUnits = getConsumedCapacityUnits(result.getConsumedCapacity(),
                DEFAULT_PUT_CONSUMED_CAPACITY_UNITS);
        putIntoStream(
                request.getTableName(),
                "PutItem",
                consumedCapacityUnits.doubleValue(),
                request.getItem()::get,
                startTime,
                endTime
        );
    }

    private void putIntoStream(final GetItemRequest request,
                               final GetItemResult result,
                               final Instant startTime,
                               final Instant endTime) {
        final Double consumedCapacityUnits = getConsumedCapacityUnits(result.getConsumedCapacity(),
                DEFAULT_GET_CONSUMED_CAPACITY_UNITS);
        putIntoStream(
                request.getTableName(),
                "GetItem",
                consumedCapacityUnits.doubleValue(),
                request.getKey()::get,
                startTime,
                endTime
        );
    }

    private void putIntoStream(final QueryRequest request,
                               final QueryResult result,
                               final Instant startTime,
                               final Instant endTime) {
        final Double consumedCapacityUnits = getConsumedCapacityUnits(result.getConsumedCapacity(),
                DEFAULT_QUERY_CONSUMED_CAPACITY_UNITS);
        Map<String, AttributeValue> parsedExpressions = parseKeyConditionExpression(request);
        putIntoStream(
                request.getTableName(),
                "Query",
                consumedCapacityUnits.doubleValue(),
                parsedExpressions::get,
                startTime,
                endTime
        );
    }

    private void putIntoStream(final BatchWriteItemRequest request,
                               final BatchWriteItemResult result,
                               final Instant startTime,
                               final Instant endTime) {
        for (String table: request.getRequestItems().keySet()) {
            double totalConsumedCapacity;
            if (result.getConsumedCapacity() != null) {
                totalConsumedCapacity = result.getConsumedCapacity().stream()
                        .filter(c -> c.getTableName().equals(table))
                        .mapToDouble(c -> c.getCapacityUnits())
                        .findAny()
                        .orElse(DEFAULT_GET_CONSUMED_CAPACITY_UNITS);
            } else {
                totalConsumedCapacity = request.getRequestItems().get(table).size()
                        * DEFAULT_PUT_CONSUMED_CAPACITY_UNITS;
            }
            final List<WriteRequest> writeRequests = request.getRequestItems().get(table);
            final double consumedCapacityPerItem = totalConsumedCapacity / writeRequests.size();
            for (WriteRequest requestItem: writeRequests) {
                putIntoStream(
                        table,
                        "BatchWrite." + (requestItem.getPutRequest() != null
                                ? "Put"
                                : "Delete"
                        ),
                        consumedCapacityPerItem,
                        requestItem.getPutRequest() != null
                                ? requestItem.getPutRequest().getItem()::get
                                : requestItem.getDeleteRequest().getKey()::get,
                        startTime,
                        endTime
                );
            }
        }
    }

    private void putIntoStream(final BatchGetItemRequest request,
                               final BatchGetItemResult result,
                               final Instant startTime,
                               final Instant endTime) {
        for (String table: request.getRequestItems().keySet()) {
            double totalConsumedCapacity;
            if (result.getConsumedCapacity() != null) {
                totalConsumedCapacity = result.getConsumedCapacity().stream()
                        .filter(c -> c.getTableName().equals(table))
                        .mapToDouble(c -> c.getCapacityUnits())
                        .findAny()
                        .orElse(0.0);
            } else {
                totalConsumedCapacity = request.getRequestItems().get(table).getKeys().size()
                        * DEFAULT_GET_CONSUMED_CAPACITY_UNITS;
            }
            final KeysAndAttributes keysAndAttributes = request.getRequestItems().get(table);
            final double consumedCapacityPerItem = totalConsumedCapacity / keysAndAttributes.getKeys().size();
            for (Map<String, AttributeValue> keys: keysAndAttributes.getKeys()) {
                putIntoStream(
                        table,
                        "BatchGet",
                        consumedCapacityPerItem,
                        keys::get,
                        startTime,
                        endTime
                );
            }
        }
    }

    private void putIntoStream(final UpdateItemRequest request,
                               final UpdateItemResult result,
                               final Instant startTime,
                               final Instant endTime) {
        final Double consumedCapacityUnits = getConsumedCapacityUnits(result.getConsumedCapacity(),
                DEFAULT_UPDATE_CONSUMED_CAPACITY_UNITS);
        putIntoStream(
                request.getTableName(),
                "UpdateItem",
                consumedCapacityUnits.doubleValue(),
                request.getKey()::get,
                startTime,
                endTime
        );
    }

    private void putIntoStream(final String tableName,
                               final String operation,
                               final double io,
                               final Function<String, AttributeValue> attributeValueExtractor,
                               final Instant startTime,
                               final Instant endTime) {
        final ImmutableSet<String> attributes = tablesAndAttributesToMonitor.get(tableName);
        if (attributes == null) {
            return;
        }
        try {
            byte[] serializedBlob;
            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                try (JsonWriter jsonWriter = gson.newJsonWriter(new OutputStreamWriter(byteArrayOutputStream,
                        StandardCharsets.UTF_8))) {
                    jsonWriter.beginObject();
                    jsonWriter.name("start_time").value(startTime.toEpochMilli());
                    jsonWriter.name("end_time").value(endTime.toEpochMilli());
                    jsonWriter.name("IO").value(io);
                    jsonWriter.name("Operation").value(operation);
                    jsonWriter.name("Table").value(tableName);
                    jsonWriter.name("KeyValues");
                    jsonWriter.beginArray();
                    for (String attribute : attributes) {
                        final AttributeValue attributeValue = attributeValueExtractor.apply(attribute);
                        if (attributeValue == null) {
                            continue;
                        }
                        jsonWriter.beginObject();
                        jsonWriter.name("name").value(attribute);
                        jsonWriter.name("value").value(getAttributeString(attributeValue));
                        jsonWriter.endObject();
                    }
                    jsonWriter.endArray();
                    jsonWriter.endObject();
                }
                if (log.isDebugEnabled()) {
                    log.debug("Inserting into stream: {}",
                            new String(byteArrayOutputStream.toByteArray(), StandardCharsets.UTF_8));
                }
                serializedBlob = byteArrayOutputStream.toByteArray();
            }
            // As the random integer is used for setting the partition key and not cryptographic operations,
            // ThreadLocalRandom is used instead of SecureRandom to avoid performance impact.
            kinesisClient.putRecord(streamName, ByteBuffer.wrap(serializedBlob),
                    Integer.toString(ThreadLocalRandom.current().nextInt()));
        } catch (RuntimeException | IOException ex) {
            log.error("Failed to put result into Kinesis stream", ex);
        }
    }

    private Map<String, AttributeValue> parseKeyConditionExpression(final QueryRequest request) {
        if (request.getKeyConditionExpression() == null) {
            return ImmutableMap.of();
        }
        final Matcher matcher = KEY_EXPRESSION_PATTERN.matcher(request.getKeyConditionExpression());
        final ImmutableMap.Builder<String, AttributeValue> builder = ImmutableMap.builder();
        while (matcher.find()) {
            String keyName = matcher.group("Key");
            final String valueName = matcher.group("Value");
            final AttributeValue value = request.getExpressionAttributeValues().get(valueName);
            if (value == null) {
                continue;
            }
            if (request.getExpressionAttributeNames() != null) {
                String replacedName = request.getExpressionAttributeNames().get(keyName);
                if (replacedName != null) {
                    keyName = replacedName;
                }
            }
            builder.put(keyName, value);
        }
        return builder.build();
    }


    private String getAttributeString(final AttributeValue attributeValue) {
        if (attributeValue.getS() != null) {
            return attributeValue.getS();
        } else if (attributeValue.getN() != null) {
            return attributeValue.getN();
        } else if (attributeValue.getB() != null) {
            return Base64.getEncoder().encodeToString(attributeValue.getB().array());
        } else {
            return null;
        }
    }

    /**
     * This helper method returns the capacity units from the provided ConsumedCapacity object.
     * A default is returned if useDefaultConsumedCapacity is set to true.
     * @param consumedCapacity ConsumedCapacity object
     * @param defaultConsumedCapacityUnits The default consumed capacity returned if capacity units is not provided
     *                                     in the consumedCapacity object and useDefaultConsumedCapacity is true.
     * @return A Double object for the consumed capacity units.
     */
    private Double getConsumedCapacityUnits(final ConsumedCapacity consumedCapacity,
                                            final double defaultConsumedCapacityUnits) {
        if (useDefaultConsumedCapacity) {
            return Optional.ofNullable(consumedCapacity)
                    .orElse(new ConsumedCapacity().withCapacityUnits(defaultConsumedCapacityUnits))
                    .getCapacityUnits();
        }
        return consumedCapacity.getCapacityUnits();
    }
}
