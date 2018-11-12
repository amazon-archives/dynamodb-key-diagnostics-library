# DynamoDB Key Diagnostics Library 🌡️
[DynamoDB](https://aws.amazon.com/dynamodb/) Key Diagnostics Library is a Java DynamoDB client wrapper that automatically logs your key usage information to [Kinesis](https://aws.amazon.com/kinesis/)
as your application reads/writes data from/to DynamoDB. You can then use [Kinesis Data Analytics](https://aws.amazon.com/kinesis/data-analytics/) to feed into [CloudWatch](https://aws.amazon.com/cloudwatch/)
to monitor and alarm if any single key gets too hot, and to feed into [S3](https://aws.amazon.com/s3/)/[Athena](https://aws.amazon.com/athena/)/[QuickSight](https://aws.amazon.com/quicksight/) to report on your detailed
key usage and have heatmaps to help diagnose your application.

```bash
.
├── README.md                                                   <-- This instructions file
├── LICENSE.txt                                                 <-- Apache Software License 2.0
├── NOTICE.txt                                                  <-- Copyright notices
├── checkstyle.xml                                              <-- Checkstyle for the Key Diagnostics client
├── pom.xml                                                     <-- Java dependencies, Docker integration test orchestration
├── resources
│   ├── python                                                  <-- Contains the AWS Lambda function for emitting hot key metrics and logs
│   │   ├── src
│   │   │   └── diagnostics
│   │   │       └── hot_key_logger_lambda.py                    <-- The actual Lambda function
│   │   └── tests
│   │       └── diagnostics
│   │           └── test_hot_key_logger_lambda.py               <-- Unit test for the Lambda function
│   └── DynamoDB_Key_Diagnostics_Library.yml                    <-- CloudFormation template, see "Packaging and Deployment" section below
├── src
│   ├── main
│   │   └── java
│   │       └── com.amazonaws.services.dynamodb.diagnostics     <-- Classes to manage Dagger 2 dependency injection
│   │           ├── DynamoDBKeyDiagnosticsClient.java           <-- Contains inject methods for handler entrypoints
│   │           └── DynamoDBKeyDiagnosticsClientBuidler.java    <-- Provides dependencies like the DynamoDB client for injection
│   └── test                                                    <-- Unit and integration tests
│       └── java
│           └── com.amazonaws.services.dynamodb.diagnostics
│               ├── DynamoDBKeyDiagnosticsClientIT.java         <-- Integration tests which makes requests to DynamoDB Local and writes to Kinesis Streams
│               └── DynamoDBKeyDiagnosticsClientTest.java       <-- Unit tests for the Key Diagnostics client
│
└── samples                                                     <-- Contains the Movies demo application that uses the Key Diagnostics client
     └── movies
         ├── main
         │   └── java
         │       └── com.amazonaws.services.dynamodb.diagnostics.demo
         │           └── MoviesApplication.java                 <-- Simulates hot key scenario with certain "hit movies"
         ├── checkstyle.xml                                     <-- Checkstyle for the Movies demo application
         └── pom.xml                                            <-- Java and the Key Diagnostics client dependencies
```


## Prerequisites

To use the DynamoDB Key Diagnostics Library or run the demo, you must have the following:
* Java 1.8 or later
* Maven 3 or later
* an AWS account

# Setup process

Note: At the time, the library aggregates the metrics for keys at minute and second granularity.
Depending on your business requirements, you may choose to modify the client to aggregate data differently. Currently, you can set up this post’s CloudFormation template in the following AWS Regions: US East (N Virginia), US West (Oregon), EU (Ireland), and EU (Frankfurt).

## Step 1: Instal the Key Diagnostics Library

To install the Key Diagnostics Library, run the following commands:

```shell
mvn package

mvn install:install-file \
    -Dfile=target/dynamodb-key-diagnostics-library-1.0-jar-with-dependencies.jar \
    -DgroupId=com.amazonaws.services.dynamodb.diagnostics \
    -DartifactId=dynamodb-key-diagnostics-library \
    -Dversion=1.0 \
    -Dpackaging=jar
```

## Step 2: Configure your AWS credentials

Configure your [AWS CLI](https://aws.amazon.com/cli/) credentials, if you haven't already. The following AWS resources will be synthesized under the configured account.

```shell
aws configure
```
Make sure you have [Amazon S3](https://aws.amazon.com/s3/), [AWS Lambda](https://aws.amazon.com/lambda/), [Amazon Kinesis](https://aws.amazon.com/kinesis/), [Amazon CloudWatch](https://aws.amazon.com/cloudwatch/) and CloudFormation permissions with the configured credentials.

## Step 3: Create and deploy the required AWS resources by using the CloudFormation template

You now will deploy a Lambda function for reporting and monitoring metrics.
To do this, first upload the provided Lambda function to Amazon S3.
If you don't have an Amazon S3 bucket already, [create one](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html).
(Throughout the following instructions, replace the placeholder names with your own names.)

```shell
export BUCKET_NAME=my_cool_new_bucket
aws s3 mb s3://$BUCKET_NAME
```

Then, package the provided Hot Key Lambda function the Amazon S3 bucket.

```shell
aws cloudformation package \
    --template-file resources/DynamoDB_Key_Diagnostics_Library.yaml \
    --s3-bucket $BUCKET_NAME \
    --output-template-file packaged.yaml
```

You can then create the rest of the necessary AWS resources (such as the Amazon Kinesis Data Streams stream, Amazon Kinesis Data Analytics application, and CloudWatch alarm). Also, provide a CloudFormation stack name.

```shell
STACK_NAME=KeyDiagnosticsStack

aws cloudformation deploy \
    --template-file packaged.yaml \
    --stack-name $STACK_NAME \
    --capabilities CAPABILITY_IAM
```

CloudFormation does not automatically start the Kinesis Data Analytics application, so to start the application, navigate to the Amazon Kinesis console or run the following commands.

```shell
# Find out the Kinesis Analtyics Application Name by going to the Kinesis console or `aws kinesisanalytics list-applications`
KINESIS_ANALYTICS_APP_NAME="Put your application name here"

# Then, find out the InputID
INPUT_ID=`aws kinesisanalytics describe-application \
    --application-name $KINESIS_ANALYTICS_APP_NAME \
    --query 'ApplicationDetail.InputDescriptions[0].InputId'`

# Start the Kinesis Data Analytics app
aws kinesisanalytics start-application \
    --application-name $KINESIS_ANALYTICS_APP_NAME \
    --input-configurations Id=$INPUT_ID,InputStartingPositionConfiguration={InputStartingPosition=NOW}
```

You now are ready to run the demo Movies example application in the repository (step 3.1) or change your code to use the Key Diagnostics Library (step 3.2).

## Client Usage
To use the Key Diagnostics client, you first need to create a Kinesis stream that it can log to, then you can use that stream name
along with the Kinesis client and the DynamoDB client you're wrapping to create the Key Diagnostics client. You also
need to specify which key attributes in which tables you need to monitor - the easiest way to do that is to use the
factory method that just monitors all the key attributes for all the tables and global secondary indexes in your
account:

```java
DynamoDBKeyDiagnosticsClient ddbClient = DynamoDBKeyDiagnosticsClient.monitorAllPartitionKeys(
    dynamoDB,
    kinesisClient,
    kinesisStreamName
);
```

If you do need to specify your own attributes to monitor (e.g. if you are considering creating a new global secondary
index on a new attribute and are wondering if it has hot values) then you can create it as follows:

```java
DynamoDBKeyDiagnosticsClient ddbClient = new DynamoDBKeyDiagnosticsClient(
    dynamoDB,
    kinesisClient,
    kinesisStreamName,
    ImmutableMap.of("MyTable", ImmutableList.of("MyAttribute"))
);
```

After you created the diagnostics client, you can then use it everywhere you would've used the regular AmazonDynamoDB
client (it implements the AmazonDynamoDB interface). The diagnostics client creates a thread pool to asynchronously
log the key usage information to Kinesis, so when you're done with it you should `close()` it so that it can shut down
those threads.

## Packaging and Deployment

We will synthesize the AWS resources through CloudFormation and Serverless Application Model (SAM).
One of the resources is an AWS Lambda function that emits hot key metrics and logs the hot key to CloudWatch Metrics and CloudWatch Logs respectively.
To use the Lambda function, we need a S3 bucket where we can upload our Lambda function packaged as ZIP before we deploy anything - If you don't have a S3 bucket to store code artifacts then this is a good time to create one:

```shell
export BUCKET_NAME=my_cool_new_bucket
aws s3 mb s3://$BUCKET_NAME
```

Then, we can package our Hot Key Lambda function (under `resources/python/src/diagnostics/hot_key_logger_lambda.py`) to S3:
```shell
aws cloudformation package \
    --template-file resources/DynamoDB_Key_Diagnostics_Library.yaml \
    --s3-bucket $BUCKET_NAME \
    --output-template-file packaged.yaml
```

Lastly, we can synthesize the rest of our resources with:
```shell
STACK_NAME=KeyDiagnosticsStack

aws cloudformation deploy \
    --template-file packaged.yaml \
    --stack-name $STACK_NAME \
    --capabilities CAPABILITY_IAM
```

We use [AWS Kinesis Data Analytics](https://aws.amazon.com/kinesis/data-analytics/) to aggregate your key usage.
Since CloudFormation does not automatically start the analytics application, you will have to start the application manually on the Kinesis console, or by running:



### Customizing your Kinesis Data Stream according to your DynamoDB Table

Depending on the provisioned capacity of your DynamoDB table, you may change the shard count of the Kinesis Data Stream used to process your requests. The default and minimum is 4 shards.
To override the shard count, you add the following override instead:
```shell
SHARD_COUNT=10

aws cloudformation deploy \
    --template-file packaged.yaml \
    --stack-name postreview \
    --capabilities CAPABILITY_IAM
    --parameter-overrides KinesisSourceStreamShardCount=$SHARD_COUNT
```

## Running the Demo

This demo uses the [IMDb meta-data](https://registry.opendata.aws/imdb/) dataset to create an application that rates movies by putting in items into DynamoDB.
Certain movies will be "trending", thus creating an uneven load on certain hash keys.

After you have installed the Key Diagnostics Library dependencies and setup all the AWS resources, navigate to the `samples/movies/` directory.

Then, execute the demo by running:
```shell
KINESIS_STREAM_NAME="Put your Kinesis Data Stream name here"
REGION="Put the region where the Kinesis Data Stream and DynamoDB table are set up"

mvn package exec:java@movies -Dexec.args="trend $KINESIS_STREAM_NAME $REGION"
```

## Visualization through Amazon Athena and QuickSight
If you are interested in creating dashboards or querying the key usage information, or wish to understand what the access patterns of certain attributes, we highly recommend setting up [Athena](https://aws.amazon.com/athena/) and [QuickSight](https://aws.amazon.com/quicksight/).

First, go to the Athena Console, and put in the following under **New query 1**, then click **Run Query**. This will create an Athena database for the key usage information stored on S3:

```sql
CREATE DATABASE IF NOT EXISTS dynamodbkeydiagnosticslibrary
COMMENT 'Athena database for DynamoDB Key Diagnostics Library;
```

Then, create the Athena table. Following the demo app, we will use `movies` as the table name.
If you synthesized the AWS with the provided CloudFormation template in Step 1, the S3 Location should be something similar to: s3://keydiagnosticspdx-aggresultbucket-ejkhrnvyw8ku/keydiagnostics/

```sql
CREATE EXTERNAL TABLE `movies`(
    `second` timestamp COMMENT 'Second aggregated results',
    `tablename` string COMMENT 'DynamoDB table name',
    `hashkey` string COMMENT 'The partition key attribute name',
    `hashkeyvalue` string COMMENT 'The partition key attribute value',
    `operation` string COMMENT 'DynamoDB operation',
    `totalio` float COMMENT 'Total IO consumed')
ROW FORMAT SERDE
    'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    's3://keydiagnosticspdx-aggresultbucket-ejkhrnvyw8ku/keydiagnostics/'
```

After setting the Athena table, you can use QuickSight to visualize the key usage pattern of your application:

1. Go to the QuickSight console and click Manage data on the upper right.
2. Click New data set, choose Athena and pick a data source name. Then, you should be able to select the Athena database and table created in the previous section.
3. Choose **Import to SPICE for quicker analytics**, then click **Visualize!**
4. Now you should be able to create graphs by filtering table names, time range, partition keys, operation etc. The following is a heat map that shows what movies are popular over a certain time range:

![Sample heat map on Amazon Quicksight](https://s3-us-west-2.amazonaws.com/key-diagnostics-test-data/quicksight_heat_map.png)

## Testing
### Running unit tests
We use `JUnit` for testing our code. Unit tests mock out the `AmazonDynamoDBClient` and do not require connectivity to a DynamoDB endpoint.
You can run unit tests with the following command:
```shell
mvn test
```

### Running integration tests
Integration tests do not mock out the `AmazonDynamoDBClient` and require connectivity to a DynamoDB endpoint. As such, the POM starts DynamoDB Local from the Dockerhub image for integration tests.
```shell
mvn verify
```