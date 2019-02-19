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

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.experimental.Delegate;

import java.time.Instant;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Similar to DynamoDBKeyDiagnosticsClient, this is a wrapper over a regular DynamoDB async client
 * that captures key usage information for every data API call and asynchronously sends that information
 * to a Kinesis stream that can be used to monitor for hot keys.
 *
 * @author Mesut Kirci
 */
@SuppressFBWarnings(value = "RI_REDUNDANT_INTERFACES")
public class DynamoDBKeyDiagnosticsClientAsync implements AmazonDynamoDBAsync, AutoCloseable {

    @Delegate(types = AmazonDynamoDBAsync.class, excludes = Implemented.class)
    private final AmazonDynamoDBAsync underlyingClient;

    private final KinesisStreamReporter streamReporter;

    @Override
    public void close() throws InterruptedException {
        streamReporter.close();
    }

    private interface Implemented {
        Future<BatchGetItemResult> batchGetItemAsync(
            BatchGetItemRequest batchGetItemRequest,
            AsyncHandler<BatchGetItemRequest, BatchGetItemResult> asyncHandler
        );

        Future<BatchWriteItemResult> batchWriteItemAsync(
            BatchWriteItemRequest batchWriteItemRequest,
            AsyncHandler<BatchWriteItemRequest, BatchWriteItemResult> asyncHandler
        );

        Future<GetItemResult> getItemAsync(GetItemRequest getItemRequest,
                                           AsyncHandler<GetItemRequest, GetItemResult> asyncHandler);

        Future<PutItemResult> putItemAsync(PutItemRequest putItemRequest,
                                           AsyncHandler<PutItemRequest, PutItemResult> asyncHandler);
    }

    /**
     * Constructs the wrapper client.
     *
     * @param underlyingClient The regular client we're wrapping.
     * @param streamReporter   The Kinesis stream reporter that will publish the key usage information.
     */
    DynamoDBKeyDiagnosticsClientAsync(
            final AmazonDynamoDBAsync underlyingClient,
            final KinesisStreamReporter streamReporter) {
        this.underlyingClient = checkNotNull(underlyingClient);
        this.streamReporter = checkNotNull(streamReporter);
    }

    public Future<BatchGetItemResult> batchGetItemAsync(
        final BatchGetItemRequest request,
        final AsyncHandler<BatchGetItemRequest, BatchGetItemResult> asyncHandler
    ) {
        final Instant startTime = Instant.now();
        final BatchGetItemRequest instrumented = request.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        return underlyingClient.batchGetItemAsync(instrumented,
            new AsyncHandler<BatchGetItemRequest, BatchGetItemResult>() {
                @Override
                public void onError(final Exception e) {
                    asyncHandler.onError(e);
                }

                @Override
                public void onSuccess(final BatchGetItemRequest request, final BatchGetItemResult response) {
                    final Instant endTime = Instant.now();
                    streamReporter.putIntoStream(request, response, startTime, endTime);
                    asyncHandler.onSuccess(request, response);
                }
            });
    }

    public Future<BatchWriteItemResult> batchWriteItemAsync(
        final BatchWriteItemRequest request,
        final AsyncHandler<BatchWriteItemRequest, BatchWriteItemResult> asyncHandler
    ) {
        final Instant startTime = Instant.now();
        final BatchWriteItemRequest instrumented = request.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        return underlyingClient.batchWriteItemAsync(instrumented,
            new AsyncHandler<BatchWriteItemRequest, BatchWriteItemResult>() {
                @Override
                public void onError(final Exception e) {
                    asyncHandler.onError(e);
                }

                @Override
                public void onSuccess(final BatchWriteItemRequest request, final BatchWriteItemResult response) {
                    final Instant endTime = Instant.now();
                    streamReporter.putIntoStream(request, response, startTime, endTime);
                    asyncHandler.onSuccess(request, response);
                }
            });
    }

    public Future<GetItemResult> getItemAsync(
        final GetItemRequest request,
        final AsyncHandler<GetItemRequest, GetItemResult> asyncHandler
    ) {
        final Instant startTime = Instant.now();
        final GetItemRequest instrumented = request.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        return underlyingClient.getItemAsync(instrumented,
            new AsyncHandler<GetItemRequest, GetItemResult>() {
                @Override
                public void onError(final Exception e) {
                    asyncHandler.onError(e);
                }

                @Override
                public void onSuccess(final GetItemRequest request, final GetItemResult response) {
                    final Instant endTime = Instant.now();
                    streamReporter.putIntoStream(request, response, startTime, endTime);
                    asyncHandler.onSuccess(request, response);
                }
            });
    }

    public Future<PutItemResult> putItemAsync(
        final PutItemRequest request,
        final AsyncHandler<PutItemRequest, PutItemResult> asyncHandler
    ) {
        final Instant startTime = Instant.now();
        final PutItemRequest instrumented = request.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        return underlyingClient.putItemAsync(instrumented,
            new AsyncHandler<PutItemRequest, PutItemResult>() {
                @Override
                public void onError(final Exception e) {
                    asyncHandler.onError(e);
                }

                @Override
                public void onSuccess(final PutItemRequest request, final PutItemResult response) {
                    final Instant endTime = Instant.now();
                    streamReporter.putIntoStream(request, response, startTime, endTime);
                    asyncHandler.onSuccess(request, response);
                }
            });
    }
}
