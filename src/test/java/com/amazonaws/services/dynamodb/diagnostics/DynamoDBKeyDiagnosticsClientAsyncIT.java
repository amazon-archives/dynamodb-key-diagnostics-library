package com.amazonaws.services.dynamodb.diagnostics;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DynamoDBKeyDiagnosticsClientAsyncIT {
    private static final Regions REGION = Regions.US_WEST_2;
    private static final int TEST_IDENTIFIER = ThreadLocalRandom.current().nextInt(0, 100000);
    private static final String STREAM_NAME = "thermometer-test-stream-" + TEST_IDENTIFIER;
    private static final String HASH_TABLE_NAME = "thermometer-hash-test-table-" + TEST_IDENTIFIER;
    private static final String HASH_RANGE_TABLE_NAME = "thermometer-hash-range-test-table-" + TEST_IDENTIFIER;
    private static final AWSCredentialsProvider CREDENTIALS_PROVIDER = new DefaultAWSCredentialsProviderChain();
    private static final AmazonKinesis KINESIS_CLIENT = AmazonKinesisClientBuilder.standard()
        .withCredentials(CREDENTIALS_PROVIDER)
        .withRegion(REGION)
        .build();

    private static final String HASH_KEY_NAME = "id";
    private static final String RANGE_KEY_NAME = "range";
    private static String shardId;

    private static AmazonDynamoDBAsync amazonDynamoDBAsync;

    @BeforeClass
    public static void createResources() throws Exception {
        final String endpoint = System.getenv("ENDPOINT_OVERRIDE");
        AmazonDynamoDBAsyncClientBuilder dynamoDBBuilder = AmazonDynamoDBAsyncClientBuilder.standard()
            .withCredentials(CREDENTIALS_PROVIDER);
        if (!Strings.isNullOrEmpty(endpoint)) {
            dynamoDBBuilder = dynamoDBBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, REGION.getName()));
        } else {
            dynamoDBBuilder = dynamoDBBuilder.withRegion(REGION);
        }
        amazonDynamoDBAsync = dynamoDBBuilder.build();

        log.info("Creating stream {}", STREAM_NAME);
        KINESIS_CLIENT.createStream(STREAM_NAME, 1);
        log.info("Creating table {}", HASH_TABLE_NAME);
        amazonDynamoDBAsync.createTable(new CreateTableRequest()
            .withTableName(HASH_TABLE_NAME)
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
            .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_NAME, ScalarAttributeType.N))
            .withKeySchema(new KeySchemaElement(HASH_KEY_NAME, KeyType.HASH))
        );
        log.info("Creating table {}", HASH_RANGE_TABLE_NAME);
        amazonDynamoDBAsync.createTable(new CreateTableRequest()
            .withTableName(HASH_RANGE_TABLE_NAME)
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
            .withAttributeDefinitions(
                new AttributeDefinition(HASH_KEY_NAME, ScalarAttributeType.N),
                new AttributeDefinition(RANGE_KEY_NAME, ScalarAttributeType.N)
            )
            .withKeySchema(
                new KeySchemaElement(HASH_KEY_NAME, KeyType.HASH),
                new KeySchemaElement(RANGE_KEY_NAME, KeyType.RANGE)
            )
        );
        log.info("Waiting for resources to be active");
        DynamoDB dynamoDB = new DynamoDB(amazonDynamoDBAsync);
        dynamoDB.getTable(HASH_TABLE_NAME).waitForActive();
        dynamoDB.getTable(HASH_RANGE_TABLE_NAME).waitForActive();
        while (!KINESIS_CLIENT.describeStream(STREAM_NAME).getStreamDescription().getStreamStatus().equals("ACTIVE")) {
            Thread.sleep(100);
        }
        final ListShardsResult listShardsResult = KINESIS_CLIENT.listShards(new ListShardsRequest().withStreamName(STREAM_NAME));
        shardId = listShardsResult.getShards().get(0).getShardId();
        log.info("Stream and tables are active, shard ID is {}", shardId);
    }

    @AfterClass
    public static void deleteResources() {
        log.info("Deleting tables {} and {}", HASH_TABLE_NAME, HASH_RANGE_TABLE_NAME);
        amazonDynamoDBAsync.deleteTable(HASH_TABLE_NAME);
        amazonDynamoDBAsync.deleteTable(HASH_RANGE_TABLE_NAME);
        log.info("Deleting stream {}", STREAM_NAME);
        KINESIS_CLIENT.deleteStream(STREAM_NAME);
    }

    @Test
    public void putTest() throws Exception {
        final DynamoDBKeyDiagnosticsClientAsync thermometerClient = createClient(HASH_KEY_NAME);
        final PutItemRequest request = new PutItemRequest()
            .withTableName(HASH_TABLE_NAME)
            .withItem(ImmutableMap.of(HASH_KEY_NAME, new AttributeValue().withN("5")));
        String logged = performOperationAndGetLogged(thermometerClient.putItemAsync(request,
            createAsyncHandler(PutItemRequest.class, PutItemResult.class)));
        assertThat(logged, containsString("'IO':" + KinesisStreamReporter.DEFAULT_PUT_CONSUMED_CAPACITY_UNITS));
        assertThat(logged, containsString("'Operation':'PutItem'"));
        assertThat(logged, containsString(String.format("'Table':'%s'", HASH_TABLE_NAME)));
        assertThat(logged, containsString("'KeyValues':[{'name':'id','value':'5'}]"));
    }

    @Test
    public void getTest() throws Exception {
        final DynamoDBKeyDiagnosticsClientAsync thermometerClient = createClient(HASH_KEY_NAME);
        final GetItemRequest request = new GetItemRequest()
            .withTableName(HASH_TABLE_NAME)
            .withKey(ImmutableMap.of(
                HASH_KEY_NAME, new AttributeValue().withN("5")
            ));
        String logged = performOperationAndGetLogged(thermometerClient.getItemAsync(request,
            createAsyncHandler(GetItemRequest.class, GetItemResult.class)));
        assertThat(logged, containsString("'IO':" + KinesisStreamReporter.DEFAULT_GET_CONSUMED_CAPACITY_UNITS));
        assertThat(logged, containsString("'Operation':'GetItem'"));
        assertThat(logged, containsString(String.format("'Table':'%s'", HASH_TABLE_NAME)));
        assertThat(logged, containsString("'KeyValues':[{'name':'id','value':'5'}]"));
    }

    @Test
    public void getTestHashRange() throws Exception {
        final DynamoDBKeyDiagnosticsClientAsync thermometerClient = createClient(HASH_KEY_NAME, RANGE_KEY_NAME);
        final GetItemRequest request = new GetItemRequest()
            .withTableName(HASH_RANGE_TABLE_NAME)
            .withKey(ImmutableMap.of(
                HASH_KEY_NAME, new AttributeValue().withN("5"),
                RANGE_KEY_NAME, new AttributeValue().withN("6")
            ));
        String logged = performOperationAndGetLogged(thermometerClient.getItemAsync(request,
            createAsyncHandler(GetItemRequest.class, GetItemResult.class)));
        assertThat(logged, containsString("'IO':" + KinesisStreamReporter.DEFAULT_GET_CONSUMED_CAPACITY_UNITS));
        assertThat(logged, containsString("'Operation':'GetItem'"));
        assertThat(logged, containsString(String.format("'Table':'%s'", HASH_RANGE_TABLE_NAME)));
        assertThat(logged, containsString("{'name':'id','value':'5'}"));
        assertThat(logged, containsString("{'name':'range','value':'6'}"));
    }

    @Test
    public void batchWriteTest() throws Exception {
        final DynamoDBKeyDiagnosticsClientAsync thermometerClient = createClient(HASH_KEY_NAME, RANGE_KEY_NAME);
        final BatchWriteItemRequest request = new BatchWriteItemRequest()
            .withRequestItems(ImmutableMap.of(
                HASH_TABLE_NAME, ImmutableList.of(
                    new WriteRequest(new PutRequest(ImmutableMap.of(
                        HASH_KEY_NAME, new AttributeValue().withN("10")
                    ))),
                    new WriteRequest(new DeleteRequest(ImmutableMap.of(
                        HASH_KEY_NAME, new AttributeValue().withN("20")
                    )))
                ),
                HASH_RANGE_TABLE_NAME, ImmutableList.of(
                    new WriteRequest(new DeleteRequest(ImmutableMap.of(
                        HASH_KEY_NAME, new AttributeValue().withN("30"),
                        RANGE_KEY_NAME, new AttributeValue().withN("40")
                    )))
                )
            ));
        List<String> logged = performOperationAndGetLogged(thermometerClient.batchWriteItemAsync(request,
            createAsyncHandler(BatchWriteItemRequest.class, BatchWriteItemResult.class)),
            3).stream().sorted().collect(Collectors.toList());
        assertThat(logged.get(0), containsString("'IO':" + KinesisStreamReporter.DEFAULT_DELETE_CONSUMED_CAPACITY_UNITS));
        assertThat(logged.get(0), containsString("'Operation':'BatchWrite.Delete'"));
        assertThat(logged.get(0), containsString(String.format("'Table':'%s'", HASH_RANGE_TABLE_NAME)));
        assertThat(logged.get(0), containsString("{'name':'id','value':'30'}"));
        assertThat(logged.get(0), containsString("{'name':'range','value':'40'}"));

        assertThat(logged.get(1), containsString("'IO':" + KinesisStreamReporter.DEFAULT_DELETE_CONSUMED_CAPACITY_UNITS));
        assertThat(logged.get(1), containsString("'Operation':'BatchWrite.Delete'"));
        assertThat(logged.get(1), containsString(String.format("'Table':'%s'", HASH_TABLE_NAME)));
        assertThat(logged.get(1), containsString(
            "'KeyValues':[{'name':'id','value':'20'}]"));

        assertThat(logged.get(2), containsString("'IO':" + KinesisStreamReporter.DEFAULT_PUT_CONSUMED_CAPACITY_UNITS));
        assertThat(logged.get(2), containsString("'Operation':'BatchWrite.Put'"));
        assertThat(logged.get(2), containsString(String.format("'Table':'%s'", HASH_TABLE_NAME)));
        assertThat(logged.get(2), containsString(
            "'KeyValues':[{'name':'id','value':'10'}]"));
    }

    @Test
    public void batchGetTest() throws Exception {
        final DynamoDBKeyDiagnosticsClientAsync thermometerClient = createClient(HASH_KEY_NAME, RANGE_KEY_NAME);
        final BatchGetItemRequest request = new BatchGetItemRequest()
            .withRequestItems(ImmutableMap.of(
                HASH_TABLE_NAME, new KeysAndAttributes().withKeys(
                    ImmutableMap.of(HASH_KEY_NAME, new AttributeValue().withN("10")),
                    ImmutableMap.of(HASH_KEY_NAME, new AttributeValue().withN("20"))
                ),
                HASH_RANGE_TABLE_NAME, new KeysAndAttributes().withKeys(
                    ImmutableMap.of(
                        HASH_KEY_NAME, new AttributeValue().withN("30"),
                        RANGE_KEY_NAME, new AttributeValue().withN("40")
                    )
                )
            ));
        List<String> logged = performOperationAndGetLogged(thermometerClient.batchGetItemAsync(request,
            createAsyncHandler(BatchGetItemRequest.class, BatchGetItemResult.class)),
            3).stream().sorted().collect(Collectors.toList());
        assertThat(logged.get(0), containsString("'IO':" + KinesisStreamReporter.DEFAULT_GET_CONSUMED_CAPACITY_UNITS));
        assertThat(logged.get(0), containsString("'Operation':'BatchGet'"));
        assertThat(logged.get(0), containsString(String.format("'Table':'%s'", HASH_RANGE_TABLE_NAME)));
        assertThat(logged.get(0), containsString("{'name':'id','value':'30'}"));
        assertThat(logged.get(0), containsString("{'name':'range','value':'40'}"));

        assertThat(logged.get(1), containsString("'IO':" + KinesisStreamReporter.DEFAULT_GET_CONSUMED_CAPACITY_UNITS));
        assertThat(logged.get(1), containsString("'Operation':'BatchGet'"));
        assertThat(logged.get(1), containsString(String.format("'Table':'%s'", HASH_TABLE_NAME)));
        assertThat(logged.get(1), containsString(
            "'KeyValues':[{'name':'id','value':'10'}]"));

        assertThat(logged.get(2), containsString("'IO':" + KinesisStreamReporter.DEFAULT_GET_CONSUMED_CAPACITY_UNITS));
        assertThat(logged.get(2), containsString("'Operation':'BatchGet'"));
        assertThat(logged.get(2), containsString(String.format("'Table':'%s'", HASH_TABLE_NAME)));
        assertThat(logged.get(2), containsString(
            "'KeyValues':[{'name':'id','value':'20'}]"));
    }

    private DynamoDBKeyDiagnosticsClientAsync createClient(final String... keys) {
        return DynamoDBKeyDiagnosticsClientBuilder.standard(STREAM_NAME)
            .withUnderlyingAsyncClient(amazonDynamoDBAsync)
            .withKinesisClient(KINESIS_CLIENT)
            .withDefaultConsumedCapacity(true)
            .addKeysToMonitor(HASH_TABLE_NAME, ImmutableList.copyOf(keys))
            .addKeysToMonitor(HASH_RANGE_TABLE_NAME, ImmutableList.copyOf(keys))
            .buildAsync();
    }

    private <REQUEST extends AmazonWebServiceRequest, RESULT> AsyncHandler<REQUEST, RESULT> createAsyncHandler(
        Class<REQUEST> requestClass,
        Class<RESULT> resultClass
    ) {
        return new AsyncHandler<REQUEST, RESULT>() {
            @Override
            public void onError(Exception e) {
            }

            @Override
            public void onSuccess(REQUEST request, RESULT result) {
            }
        };
    }

    private <T> String performOperationAndGetLogged(final Future<T> dynamoInteraction) throws InterruptedException, ExecutionException {
        return performOperationAndGetLogged(dynamoInteraction, 1).get(0);
    }

    private <T> List<String> performOperationAndGetLogged(final Future<T> dynamoInteraction, final int expectedNumber)
        throws InterruptedException, ExecutionException {

        String shardIterator = KINESIS_CLIENT.getShardIterator(new GetShardIteratorRequest()
            .withShardIteratorType(ShardIteratorType.LATEST)
            .withShardId(shardId)
            .withStreamName(STREAM_NAME)
        ).getShardIterator();
        dynamoInteraction.get();
        final Stopwatch timer = Stopwatch.createStarted();
        final List<Record> allRecords = new ArrayList<>(expectedNumber);
        while (true) {
            GetRecordsResult getRecordsResult = KINESIS_CLIENT.getRecords(new GetRecordsRequest()
                .withShardIterator(shardIterator)
                .withLimit(expectedNumber)
            );
            final List<Record> currentRecords = getRecordsResult.getRecords();
            shardIterator = getRecordsResult.getNextShardIterator();
            allRecords.addAll(currentRecords);
            assertThat(allRecords.size(), lessThanOrEqualTo(expectedNumber));
            if (allRecords.size() == expectedNumber) {
                break;
            }
            assertFalse("Timed out waiting for Kinesis records", timer.elapsed(TimeUnit.SECONDS) > 3);
            Thread.sleep(1);
        }
        return allRecords.stream()
            .map(r -> new String(r.getData().array(), StandardCharsets.UTF_8).replace('\"', '\''))
            .collect(Collectors.toList());
    }
}
