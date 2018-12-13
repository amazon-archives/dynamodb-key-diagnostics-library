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

package com.amazonaws.services.dynamodb.diagnostics.demo;

import com.amazonaws.services.dynamodb.diagnostics.DynamoDBKeyDiagnosticsClient;
import com.amazonaws.services.dynamodb.diagnostics.DynamoDBKeyDiagnosticsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InvalidObjectException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Demo application that uses a movie data set to target traffic based on popularity of the movie (so more popular
 * movies get more traffic) to a simple Like/Dislike DynamoDB table, with the functionality to randomly choose a movie
 * to be a "surprise hit" and drive much more traffic to that (hot key problem).
 *
 * @author Mostafa Elhemali
 * @author Ryan Chan
 */
@Slf4j
final class MovieApplication {
    // DynamoDB table definitions
    private static final String MOVIES_WITH_COUNTRIES_TABLE_NAME = "movies_with_countries";
    private static final Long MOVIES_WITH_COUNTRIES_RCU = 100L;
    private static final Long MOVIES_WITH_COUNTRIES_WCU = 800L;
    private static final String COUNTRY_GLOBAL_INDEX_NAME = "country-index";
    private static final Long COUNTRY_GLOBAL_INDEX_RCU = 100L;
    private static final Long COUNTRY_GLOBAL_INDEX_WCU = 800L;
    private static final String MOVIE_NAME_ATTRIBUTE_NAME = "movie-name";
    private static final String COUNTRY_ATTRIBUTE_NAME = "country";
    private static final String LIKES_ATTRIBUTE_NAME = "likes";
    private static final String DISLIKES_ATTRIBUTE_NAME = "dislikes";
    private static final long DEFAULT_WAIT_TIME_FOR_CREATE_TABLE = 60;

    // The number of threads to use to drive background traffic.
    private static final int NUM_BACKGROUND_TRAFFIC_THREADS = 10;
    // How many ratings to target per second for the background traffic.
    private static final double BACKGROUND_TRAFFIC_RATE = 500.0;
    // The number of threads to use in our "surprise hit" traffic.
    private static final int NUM_SURPRISE_HIT_THREADS = 5;
    // How many ratings to target per second for the surprise hit traffic.
    private static final double SURPRISE_HIT_RATE = 1000.0;
    // How many movies are browsed (Query) per second.
    private static final double BROWSE_RATE = 100;
    private static final int NUM_ARGS = 3;
    // Probability that I will like a movie (we're generous and tend to like more than dislike movies).
    private static final double PROBABILITY_TO_LIKE_MOVIE = 0.8;

    // Movies data stored on S3
    private static final String MOVIE_DATA_BUCKET_NAME = "key-diagnostics-test-data";
    private static final String MOVIE_DATA_KEY_NAME = "movies_ratings.csv";
    private static final String MOVIE_DATA_BUCKET_REGION = "us-west-2";

    private MovieApplication() { }

    public static void main(final String[] args) throws IOException, InterruptedException {
        log.info("loggerName: " + log.toString());
        if (args.length != NUM_ARGS) {
            log.error("Usage: movies <TEST-NAME> <STREAM-NAME> <REGION>");
            System.exit(22); //EINVAL
            return;
        }
        log.info("Creating clients");
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(MOVIE_DATA_BUCKET_REGION).build();

        final String kinesisStreamName = args[1];
        final String region = args[2];
        try (DynamoDBKeyDiagnosticsClient instrumented = DynamoDBKeyDiagnosticsClientBuilder
                .standard(kinesisStreamName)
                .monitorAllPartitionKeys()
                .withKinesisClient(AmazonKinesisClientBuilder.standard().withRegion(region).build())
                .withUnderlyingClient(AmazonDynamoDBClientBuilder.standard().withRegion(region).build())
                .build()) {
            createMoviesTable(instrumented);
            switch (args[0].toLowerCase(Locale.ENGLISH)) {
                case "traffic":
                    backgroundTraffic(s3, instrumented);
                    break;
                case "trend":
                    trend(s3, instrumented);
                    break;
                default:
                    log.error("Unknown test: " + args[0]);
                    System.exit(22); //EINVAL
            }
        }
        log.info("Done");
    }

    /**
     * Helper method to create the DynamoDB table used for the demo if it does not exist already.
     */
    private static void createMoviesTable(AmazonDynamoDB dynamoDBClient) {
        try {
            dynamoDBClient.describeTable(MOVIES_WITH_COUNTRIES_TABLE_NAME);
        } catch (ResourceNotFoundException ex) {
            dynamoDBClient.createTable(
                    new CreateTableRequest()
                            .withTableName(MOVIES_WITH_COUNTRIES_TABLE_NAME)
                            .withAttributeDefinitions(
                                    new AttributeDefinition()
                                            .withAttributeName(MOVIE_NAME_ATTRIBUTE_NAME)
                                            .withAttributeType(ScalarAttributeType.S),
                                    new AttributeDefinition()
                                            .withAttributeName(COUNTRY_ATTRIBUTE_NAME)
                                            .withAttributeType(ScalarAttributeType.S)
                            )
                            .withKeySchema(new KeySchemaElement().withAttributeName(MOVIE_NAME_ATTRIBUTE_NAME).withKeyType(KeyType.HASH))
                            .withGlobalSecondaryIndexes(
                                    new GlobalSecondaryIndex()
                                            .withIndexName(COUNTRY_GLOBAL_INDEX_NAME)
                                            .withKeySchema(
                                                    new KeySchemaElement()
                                                            .withAttributeName(COUNTRY_ATTRIBUTE_NAME)
                                                            .withKeyType(KeyType.HASH),
                                                    new KeySchemaElement()
                                                            .withAttributeName(MOVIE_NAME_ATTRIBUTE_NAME)
                                                            .withKeyType(KeyType.RANGE))
                                            .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY))
                                            .withProvisionedThroughput(
                                                    new ProvisionedThroughput(MOVIES_WITH_COUNTRIES_RCU, MOVIES_WITH_COUNTRIES_WCU))
                            )
                            .withProvisionedThroughput(new ProvisionedThroughput(COUNTRY_GLOBAL_INDEX_RCU, COUNTRY_GLOBAL_INDEX_WCU))
            );
            waitForTableActive(dynamoDBClient, MOVIES_WITH_COUNTRIES_TABLE_NAME, DEFAULT_WAIT_TIME_FOR_CREATE_TABLE);
        }
    }

    /**
     * Choose a random movie and drive lots of traffic to that.
     */
    private static void trend(final AmazonS3 s3,
                              final AmazonDynamoDB dynamoDB) throws IOException, InterruptedException {
        log.info("Reading movies");
        final ImmutableList<Movie> movies = readMovies(s3);
        final Movie surpriseHit = movies.get(new SecureRandom().nextInt(movies.size()));
        log.info("Chose {} (country: {}) as my surprise trending hit. Starting worker threads",
                surpriseHit.getName(), surpriseHit.getCountry());
        final RateLimiter rateLimiter = RateLimiter.create(SURPRISE_HIT_RATE);
        final RateLimiter browsingRateLimiter = RateLimiter.create(BROWSE_RATE);
        final List<Thread> hitTrafficThreads = IntStream.range(0, NUM_SURPRISE_HIT_THREADS)
                .mapToObj(i -> new Thread(() -> surpriseHitThread(surpriseHit, rateLimiter, dynamoDB),
                        "hit-traffic-" + i))
                .collect(Collectors.toList());
        hitTrafficThreads.addAll(IntStream.range(0, NUM_BACKGROUND_TRAFFIC_THREADS)
                .mapToObj(i -> new Thread(() -> browsingThread(surpriseHit, browsingRateLimiter, dynamoDB),
                        "background-browsing-traffic-" + i))
                .collect(Collectors.toList()));
        hitTrafficThreads.forEach(Thread::start);
        log.info("Joining the threads");
        for (Thread t: hitTrafficThreads) {
            t.join();
        }
    }

    /**
     * Drive background traffic (random likes and dislikes) to the table.
     */
    private static void backgroundTraffic(final AmazonS3 s3,
                                          final AmazonDynamoDB dynamoDB) throws IOException, InterruptedException {
        log.info("Reading movies");
        final ImmutableList<Movie> movies = readMovies(s3);
        log.info("Building distribution");
        // Build a distribution of the movies so that more popular movies (the ones that got more votes in our
        // dataset) get more traffic.
        final double totalVotes = movies.stream().mapToDouble(Movie::getVotes).sum();
        final EnumeratedDistribution<Movie> movieDistribution = new EnumeratedDistribution<Movie>(
                movies.stream()
                        .map(m -> new Pair<Movie, Double>(m, m.getVotes() / totalVotes))
                        .collect(Collectors.toList())
        );
        log.info("Starting worker threads");
        final RateLimiter rateLimiter = RateLimiter.create(BACKGROUND_TRAFFIC_RATE);
        final RateLimiter browsingRateLimiter = RateLimiter.create(BROWSE_RATE);
        final List<Thread> backgroundTrafficThreads = IntStream.range(0, NUM_BACKGROUND_TRAFFIC_THREADS)
                .mapToObj(i -> new Thread(() -> backgroundTrafficThread(movieDistribution, rateLimiter, dynamoDB),
                        "background-traffic-" + i))
                .collect(Collectors.toList());
        backgroundTrafficThreads.addAll(IntStream.range(0, NUM_BACKGROUND_TRAFFIC_THREADS)
                .mapToObj(i -> new Thread(() -> backgroundBrowsingThread(
                        movieDistribution,
                        browsingRateLimiter,
                        dynamoDB),
                        "background-browsing-traffic-" + i))
                .collect(Collectors.toList()));
        backgroundTrafficThreads.forEach(Thread::start);
        log.info("Joining the threads");
        for (Thread t: backgroundTrafficThreads) {
            t.join();
        }
    }

    private static void surpriseHitThread(final Movie hitMovie,
                                          final RateLimiter rateLimiter,
                                          final AmazonDynamoDB dynamoDB) {
        while (!Thread.interrupted()) {
            try {
                rateLimiter.acquire();
                rateMovie(dynamoDB, hitMovie);
            } catch (RuntimeException ex) {
                log.error("Error in surprise hit traffic", ex);
            }
        }
    }

    private static void browsingThread(final Movie wantedMovie,
                                       final RateLimiter rateLimiter,
                                       final AmazonDynamoDB dynamoDB) {
        while (!Thread.interrupted()) {
            try {
                rateLimiter.acquire();
                browseMovie(dynamoDB, wantedMovie);
            } catch (RuntimeException ex) {
                log.error("Error in surprise hit traffic", ex);
            }
        }
    }

    private static void backgroundBrowsingThread(final EnumeratedDistribution<Movie> movieDistribution,
                                                 final RateLimiter rateLimiter,
                                                 final AmazonDynamoDB dynamoDB) {
        while (!Thread.interrupted()) {
            try {
                rateLimiter.acquire();
                Movie wantedMovie = movieDistribution.sample();
                browseMovie(dynamoDB, wantedMovie);
            } catch (RuntimeException ex) {
                log.error("Error in surprise hit traffic", ex);
            }
        }
    }

    private static void backgroundTrafficThread(final EnumeratedDistribution<Movie> movieDistribution,
                                                final RateLimiter rateLimiter,
                                                final AmazonDynamoDB dynamoDB) {
        while (!Thread.interrupted()) {
            try {
                rateLimiter.acquire();
                Movie wantedMovie = movieDistribution.sample();
                rateMovie(dynamoDB, wantedMovie);
            } catch (RuntimeException ex) {
                log.error("Error in background traffic", ex);
            }
        }
    }

    private static void browseMovie(final AmazonDynamoDB dynamoDB,
                                    final Movie wantedMovie) {
        dynamoDB.query(new QueryRequest()
                .withTableName(MOVIES_WITH_COUNTRIES_TABLE_NAME)
                .withIndexName(COUNTRY_GLOBAL_INDEX_NAME)
                .withKeyConditionExpression("country = :v_country")
                .withExpressionAttributeValues(
                        Collections.singletonMap(
                                ":v_country",
                                new AttributeValue().withS(wantedMovie.getCountry()))));
    }

    /**
     * Randomly like/dislike a given movie and record that in our DynamoDB table.
     */
    private static void rateMovie(final AmazonDynamoDB dynamoDB,
                                  final Movie wantedMovie) {
        AttributeValue movieNameAttribute = new AttributeValue().withS(wantedMovie.getName());
        GetItemResult item = dynamoDB.getItem(
                MOVIES_WITH_COUNTRIES_TABLE_NAME,
                ImmutableMap.of(MOVIE_NAME_ATTRIBUTE_NAME, movieNameAttribute));
        long currentLikes;
        long currentDislikes;
        if (item.getItem() == null) {
            currentLikes = currentDislikes = 0;
        } else {
            currentLikes = Long.parseLong(item.getItem().get(LIKES_ATTRIBUTE_NAME).getN());
            currentDislikes = Long.parseLong(item.getItem().get(DISLIKES_ATTRIBUTE_NAME).getN());
        }
        if (ThreadLocalRandom.current().nextDouble() < PROBABILITY_TO_LIKE_MOVIE) {
            currentLikes++;
        } else {
            currentDislikes++;
        }
        dynamoDB.putItem(MOVIES_WITH_COUNTRIES_TABLE_NAME, ImmutableMap.of(
                MOVIE_NAME_ATTRIBUTE_NAME, movieNameAttribute,
                COUNTRY_ATTRIBUTE_NAME, new AttributeValue().withS(wantedMovie.getCountry()),
                LIKES_ATTRIBUTE_NAME, new AttributeValue().withN(Long.toString(currentLikes)),
                DISLIKES_ATTRIBUTE_NAME, new AttributeValue().withN(Long.toString(currentDislikes))
        ));
    }

    @Value
    private static class Movie {
        private final String name;
        private final String country;
        private final double votes;
        private final double rank;
    }

    private enum MovieCsvFields {
        ID,
        COUNTRY,
        NAME,
        VOTES,
        RANK
    }

    private static ImmutableList<Movie> readMovies(final AmazonS3 s3) throws IOException {
        ImmutableList.Builder<Movie> builder = ImmutableList.builder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                s3.getObject(MOVIE_DATA_BUCKET_NAME, MOVIE_DATA_KEY_NAME).getObjectContent(),
                StandardCharsets.UTF_8))) {
            reader.readLine(); // Skip the header
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\\|");
                if (fields.length != MovieCsvFields.values().length) {
                    throw new InvalidObjectException("Invalid line: " + line);
                }
                builder.add(new Movie(
                        fields[MovieCsvFields.NAME.ordinal()],
                        fields[MovieCsvFields.COUNTRY.ordinal()],
                        Double.parseDouble(fields[MovieCsvFields.VOTES.ordinal()]),
                        Double.parseDouble(fields[MovieCsvFields.RANK.ordinal()])
                ));
            }
        }
        return builder.build();
    }

    private static void waitForTableActive(AmazonDynamoDB dynamoDB, String tableName, long waitTimeSeconds) {
        if (waitTimeSeconds < 0) {
            throw new IllegalArgumentException("Invalid waitTimeSeconds " + waitTimeSeconds);
        }

        try {
            long startTimeMs = System.currentTimeMillis();
            long elapsedMs;
            do {
                DescribeTableResult describe = dynamoDB.describeTable(new DescribeTableRequest().withTableName(tableName));
                String status = describe.getTable().getTableStatus();
                if (TableStatus.ACTIVE.toString().equals(status)) {
                    return;
                }
                if (TableStatus.DELETING.toString().equals(status)) {
                    log.error("Table " + tableName + " is " + status + ", and waiting for it to become ACTIVE is not useful.");
                    System.exit(22); //EINVAL
                }
                Thread.sleep(10 * 1000);
                elapsedMs = System.currentTimeMillis() - startTimeMs;
            } while (elapsedMs / 1000.0 < waitTimeSeconds);
        } catch (InterruptedException e) {
            log.error("Caught InterruptedException.", e);
            System.exit(22); //EINVAL
        }
        log.error("Table " + tableName + " did not become ACTIVE after " + waitTimeSeconds + " seconds.");
        System.exit(22); //EINVAL
    }
}
