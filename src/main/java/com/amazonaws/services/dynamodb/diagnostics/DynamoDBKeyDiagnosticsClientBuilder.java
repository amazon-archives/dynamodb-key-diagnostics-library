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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Client builder for the {@link DynamoDBKeyDiagnosticsClient} client.
 */
public class DynamoDBKeyDiagnosticsClientBuilder {
    public static final int DEFAULT_NUMBER_OF_STREAM_PUTTER_THREADS = 10;

    private AmazonDynamoDB underlyingClient;
    private AmazonDynamoDBAsync underlyingClientAsync;
    private AmazonKinesis kinesisClient;
    private String streamName;
    private final Map<String, Set<String>> keysToMonitorBuilder = new HashMap<>();
    private ExecutorService streamPutterService;
    private boolean useDefaultConsumedCapacity = false;

    private DynamoDBKeyDiagnosticsClientBuilder() { }

    /**
     * Creates a default client using default credential chains for the underlying clients and monitoring
     * all existing table and index keys.
     * @param kinesisStreamName The Kinesis stream name to use for key usage information.
     * @return The created default client.
     */
    public static DynamoDBKeyDiagnosticsClient defaultClient(final String kinesisStreamName) {
        return standard(kinesisStreamName).monitorAllPartitionKeys().build();
    }

    /**
     * Creates a default async client using default credential chains for the underlying clients and monitoring
     * all existing table and index keys.
     * @param kinesisStreamName The Kinesis stream name to use for key usage information.
     * @return The created default async client.
     */
    public static DynamoDBKeyDiagnosticsClientAsync defaultAsyncClient(final String kinesisStreamName) {
        return standard(kinesisStreamName).monitorAllPartitionKeys().buildAsync();
    }

    /**
     * Creates a standard client builder using default credential chains for the underlying clients and no monitored
     * keys (should be filled later using{@link DynamoDBKeyDiagnosticsClientBuilder#addKeysToMonitor(String, Collection)})
     * @param kinesisStreamName The Kinesis stream name to use for key usage information.
     * @return The client builder.
     */
    public static DynamoDBKeyDiagnosticsClientBuilder standard(final String kinesisStreamName) {
        return new DynamoDBKeyDiagnosticsClientBuilder()
                .withUnderlyingClient(AmazonDynamoDBClientBuilder.defaultClient())
                .withUnderlyingAsyncClient(AmazonDynamoDBAsyncClientBuilder.defaultClient())
                .withKinesisClient(AmazonKinesisClientBuilder.defaultClient())
                .withKinesisStreamName(kinesisStreamName)
                .withFixedStreamPutterThreadPool(DEFAULT_NUMBER_OF_STREAM_PUTTER_THREADS);
    }

    /**
     * Builds the client.
     * @return The created client.
     */
    public DynamoDBKeyDiagnosticsClient build() {
        return new DynamoDBKeyDiagnosticsClient(underlyingClient, createStreamReporter());
    }

    /**
     * Builds the client.
     * @return The created client.
     */
    public DynamoDBKeyDiagnosticsClientAsync buildAsync() {
        return new DynamoDBKeyDiagnosticsClientAsync(underlyingClientAsync, createStreamReporter());
    }

    private KinesisStreamReporter createStreamReporter() {
        return new KinesisStreamReporter(
            kinesisClient,
            streamName,
            keysToMonitorBuilder.entrySet().stream()
                .collect(ImmutableMap.toImmutableMap(e -> e.getKey(), e -> ImmutableSet.copyOf(e.getValue()))),
            streamPutterService,
            useDefaultConsumedCapacity
        );
    }

    /**
     * Sets the client to use a fixed thread pool to asynchronously put key usage information into Kinesis.
     * @param numberOfThreads The number of threads to use in the thread pool.
     * @return This object for method chaining.
     */
    public DynamoDBKeyDiagnosticsClientBuilder withFixedStreamPutterThreadPool(final int numberOfThreads) {
        return withStreamPutterExecutorService(Executors.newFixedThreadPool(numberOfThreads));
    }

    /**
     * Sets the client to use the given executor service to asynchronously put key usage information into Kinesis.
     * @param streamPutterExecutorService The executor service to use.
     * @return This object for method chaining.
     */
    public DynamoDBKeyDiagnosticsClientBuilder withStreamPutterExecutorService(
            final ExecutorService streamPutterExecutorService) {
        this.streamPutterService = checkNotNull(streamPutterExecutorService);
        return this;
    }

    /**
     * Use the given client to perform the actual operations on DynamoDB.
     * @param underlyingClient The underlying client to use to talk to DynamoDB.
     * @return This object for method chaining.
     */
    public DynamoDBKeyDiagnosticsClientBuilder withUnderlyingClient(final AmazonDynamoDB underlyingClient) {
        this.underlyingClient = checkNotNull(underlyingClient);
        return this;
    }

    /**
     * Use the given async client to perform the actual operations on DynamoDB.
     * @param underlyingClient The underlying async client to use to talk to DynamoDB.
     * @return This object for method chaining.
     */
    public DynamoDBKeyDiagnosticsClientBuilder withUnderlyingAsyncClient(final AmazonDynamoDBAsync underlyingClient) {
        this.underlyingClientAsync = checkNotNull(underlyingClient);
        return this;
    }

    /**
     * Use the given Kinesis client to put key usage information into Kinesis.
     * @param kinesisClient The Kinesis client to use.
     * @return This object for method chaining.
     */
    public DynamoDBKeyDiagnosticsClientBuilder withKinesisClient(final AmazonKinesis kinesisClient) {
        this.kinesisClient = checkNotNull(kinesisClient);
        return this;
    }

    /**
     * Use the given Kinesis stream name for key usage information.
     * @param kinesisStreamName The Kinesis stream name to use.
     * @return This object for method chaining.
     */
    public DynamoDBKeyDiagnosticsClientBuilder withKinesisStreamName(final String kinesisStreamName) {
        this.streamName = checkNotNull(kinesisStreamName);
        return this;
    }

    /**
     * If true, assume each of the DynamoDB operations consumes capacity.
     * @param defaultConsumedCapacity If true, the DynamoDBKeyDiagnosticsClient assumes all DynamoDB operations will consume a
     *                                default capacity, when consumed capacity units are not returned.
     *                                For example, you may use this option to test against DynamoDB Local (where consumed capacity
     *                                is not returned).
     *                                DynamoDBKeyDiagnosticsClient will put the default consumed capacity into the Kinesis Data Stream.
     * @return This object for method chaining.
     */
    public DynamoDBKeyDiagnosticsClientBuilder withDefaultConsumedCapacity(final boolean defaultConsumedCapacity) {
        this.useDefaultConsumedCapacity = defaultConsumedCapacity;
        return this;
    }

    /**
     * Add all the partition keys for all the tables and global secondary indexes currently in the DynamoDB account.
     * @return This object for method chaining.
     */
    public DynamoDBKeyDiagnosticsClientBuilder monitorAllPartitionKeys() {
        final DynamoDB dynamoDB = new DynamoDB(underlyingClient);
        for (Table table: dynamoDB.listTables()) {
            table.describe();
            final Set<String> tableAttributesBuilder = getTableKeySet(table.getTableName());
            final KeySchemaElement partitionKey = table.getDescription().getKeySchema().get(0);
            tableAttributesBuilder.add(partitionKey.getAttributeName());
            if (table.getDescription().getGlobalSecondaryIndexes() != null) {
                for (GlobalSecondaryIndexDescription index : table.getDescription().getGlobalSecondaryIndexes()) {
                    final KeySchemaElement indexPartitionKey = index.getKeySchema().get(0);
                    tableAttributesBuilder.add(indexPartitionKey.getAttributeName());
                }
            }
        }
        return this;
    }

    /**
     * Add a key attribute to monitor.
     * @param tableName The DynamoDB table name.
     * @param keyName The attribute name within the DynamoDB table to monitor.
     * @return This object for method chaining.
     */
    DynamoDBKeyDiagnosticsClientBuilder addKeyToMonitor(final String tableName,
                                                        final String keyName) {
        getTableKeySet(tableName).add(keyName);
        return this;
    }

    /**
     * Add key attributes to monitor.
     * @param tableName The DynamoDB table name.
     * @param keyNames The attribute names within the DynamoDB table to monitor.
     * @return The object for method chaining.
     */
    DynamoDBKeyDiagnosticsClientBuilder addKeysToMonitor(final String tableName,
                                                         final Collection<String> keyNames) {
        getTableKeySet(tableName).addAll(keyNames);
        return this;
    }

    private Set<String> getTableKeySet(String tableName) {
        return keysToMonitorBuilder.computeIfAbsent(tableName, k -> new HashSet<>());
    }
}
