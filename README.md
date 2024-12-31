# flink-connector-tdengine

Apache Flink is an open-source distributed stream batch integrated processing framework supported by the Apache Software Foundation, which can be used for many big data processing scenarios such as stream processing, batch processing, complex event processing, real-time data warehouse construction, and providing real-time data support for machine learning. At the same time, Flink has a wealth of connectors and various tools that can interface with numerous different types of data sources to achieve data reading and writing. In the process of data processing, Flink also provides a series of reliable fault-tolerant mechanisms, effectively ensuring that tasks can run stably and continuously even in the event of unexpected situations.

With the help of TDengine's Flink connector, Apache Flink can seamlessly integrate with the TDengine database. On the one hand, it can accurately store the results obtained after complex calculations and deep analysis into the TDengine database, achieving efficient storage and management of data; On the other hand, it is also possible to quickly and stably read massive amounts of data from the TDengine database, and conduct comprehensive and in-depth analysis and processing on this basis, fully tapping into the potential value of the data, providing strong data support and scientific basis for enterprise decision-making, greatly improving the efficiency and quality of data processing, and enhancing the competitiveness and innovation ability of enterprises in the digital age.

## Preconditions

Prepare the following environment:
- TDengine cluster has been deployed and is running normally (both enterprise and community versions are available)
- TaosAdapter can run normally. 
- Apache Flink v1.19.0 or above is installed. Please refer to the installation of Apache Flink [Official documents](https://flink.apache.org/)

## JRE version compatibility

JRE: Supports JRE 8 and above versions.

## Supported platforms

Flink Connector supports all platforms that can run Flink 1.19 and above versions.

## Version History

|Flink Connector Version | Major Changes | TDengine Version|
| ------------------| ------------------------------------ | ---------------- |
|        1.1.0      | 1.  Support SQL queries on data in TDengine database<br/>2 Support CDC subscription to data in TDengine database<br/>3 Supports reading and writing to TDengine database using Table SQL | 3.3.5.0 and above versions|

## Exception and error codes

After the task execution fails, check the Flink task execution log to confirm the reason for the failure
Please refer to:

| Error Code       | Description                                              | Suggested Actions    |
| ---------------- |-------------------------------------------------------   | -------------------- |
|0xa000 | connection param error | connector parameter error
|0xa001 | The groupid parameter of CDC is incorrect | The groupid parameter of CDC is incorrect|
|0xa002 | wrong topic parameter for CDC | The topic parameter for CDC is incorrect|
|0xa010 | database name configuration error | database name configuration error|
|0xa011 | Table name configuration error | Table name configuration error|
|0xa012 | No data was obtained from the data source | Failed to retrieve data from the data source|
|0xa013 | value.deserializer parameter not set | No serialization method set|
|0xa014 | List of column names for target table not set | List of column names for target table not set ||
|0x2301 | Connection already closed | The connection has been closed. Check the connection status or create a new connection to execute the relevant instructions|
| 0x2302     |this operation is NOT supported currently!                      | The current interface is not supported, you can switch to other connection methods|
|0x2303 | invalid variables | The parameter is invalid. Please check the corresponding interface specification and adjust the parameter type and size|
|0x2304 | Statement is closed | Statement has already been closed. Please check if the statement is closed and reused, or if the connection is working properly|
|0x2305 | ResultSet is closed | The ResultSet has been released. Please check if the ResultSet has been released and used again|
|0x230d | parameter index out of range | parameter out of range, please check the reasonable range of the parameter|
|0x230e | Connection already closed | The connection has been closed. Please check if the connection is closed and used again, or if the connection is working properly|
|0x230f | unknown SQL type in TDengine | Please check the Data Type types supported by TDengine|
|0x2315 | Unknown tao type in tdengine | Did the correct TDengine data type be specified when converting TDengine data type to JDBC data type|
|0x2319 | user is required | Username information is missing when creating a connection|
|0x231a | password is required | Password information is missing when creating a connection|
|0x231d | can't create connection with server within | Increase connection time by adding the parameter httpConnectTimeout, or check the connection status with taosAdapter|
|0x231e | failed to complete the task within the specified time | Increase execution time by adding the parameter messageWaitTimeout, or check the connection with taosAdapter|
|0x2352 | Unsupported encoding | An unsupported character encoding set was specified under the local connection|
| 0x2353     |internal error of database,  Please see taoslog for more details | An error occurred while executing prepareStatement on the local connection. Please check the taoslog for problem localization|
|0x2354 | Connection is NULL | Connection has already been closed while executing the command on the local connection. Please check the connection with TDengine|
|0x2355 | result set is NULL | Local connection to obtain result set, result set exception, please check connection status and retry|
|0x2356 | invalid num of fields | The meta information obtained from the local connection result set does not match|
|0x2357 | empty SQL string | Fill in the correct SQL for execution|
| 0x2371     |consumer properties must not be null!                           | When creating a subscription, the parameter is empty. Please fill in the correct parameter|
|0x2375 | Topic reference has been destroyed | During the process of creating a data subscription, the topic reference was released. Please check the connection with TDengine|
| 0x2376     |failed to set consumer topic,  Topic name is empty | During the process of creating a data subscription, the subscription topic name is empty. Please check if the specified topic name is filled in correctly|
|0x2377 | Consumer reference has been destroyed | The subscription data transmission channel has been closed, please check the connection with TDengine|
|0x2378 | Consumer create error | Failed to create data subscription. Please check the taos log based on the error message to locate the problem|
|0x237a | vGroup not found in result set VGroup | Not assigned to the current consumer, due to the Rebalance mechanism, the relationship between Consumer and VGroup is not bound|

## Data type mapping

TDengine currently supports timestamp, number, character, and boolean types, and the corresponding type conversions with Flink RowData Type are as follows:

| TDengine DataType | Flink RowDataType |
| ----------------- | ------------------ |
| TIMESTAMP         | TimestampData |
| INT               | Integer       |
| BIGINT            | Long          |
| FLOAT             | Float         |
| DOUBLE            | Double        |
| SMALLINT          | Short         |
| TINYINT           | Byte          |
| BOOL              | Boolean       |
| BINARY            | byte[]        |
| NCHAR             | StringData    |
| JSON              | StringData    |
| VARBINARY         | byte[]        |
| GEOMETRY          | byte[]        |

## Instructions for use
### Flink Semantic Selection Instructions

The semantic reason for using At Least One (at least once) is:
-TDengine currently does not support transactions and cannot perform frequent checkpoint operations and complex transaction coordination.
-Due to TDengine's use of timestamps as primary keys, downstream operators of duplicate data can perform filtering operations to avoid duplicate calculations.
-Using At Least One (at least once) to ensure high data processing performance and low data latency, the setting method is as follows:

```text
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000);
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
```

If using Maven to manage a project, simply add the following dependencies in pom.xml.

```xml
<dependency>
    <groupId>com.taosdata.flink</groupId>
    <artifactId>flink-connector-tdengine</artifactId>
    <version>1.0.0</version>
</dependency>
```

The parameters for establishing a connection include URL and Properties.
The URL specification format is:

`jdbc: TAOS-WS://[host_name]:[port]/[database_name]? [user={user}|&password={password}|&timezone={timezone}]`  

Parameter description:
- User: Login TDengine username, default value is' root '.
- Password: User login password, default value 'taosdata'.
- batchErrorIgnore： true： If there is an SQL execution failure in the middle of the ExecutBatch of Statement, continue to execute the following SQL. false： Do not execute any statements after failed SQL. The default value is: false。
- HttpConnectTimeout: The connection timeout time, measured in milliseconds, with a default value of 60000.
- MessageWaitTimeout: The timeout period for a message, measured in milliseconds, with a default value of 60000.
- UseSSL: Whether SSL is used in the connection.

### Source

Source retrieves data from the TDengine database, converts it into a format and type that Flink can handle internally, and reads and distributes it in parallel, providing efficient input for subsequent data processing.
By setting the parallelism of the data source, multiple threads can read data from the data source in parallel, improving the efficiency and throughput of data reading, and fully utilizing cluster resources for large-scale data processing capabilities.

#### Source Properties

The configuration parameters in Properties are as follows:

|Parameter Name | Type | Parameter Description | Remarks|
| ----------------------- | :-----: | ------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| TDengineConfigParams. PROPERTYKEYUSER | string | Login TDengine username, default value 'root' ||
| TDengineConfigParams. PROPERTYKEY-PASSWORD | string | User login password, default value 'taosdata' ||
| TDengineConfigParams. If the downstream operator receives data of RowData type, it only needs to be set to RowData. If the user needs to customize the type, the complete class path needs to be set here|
| TDengineConfigParams. TD_STACTMODE | boolean | This parameter is used to batch push data to downstream operators. If set to True, when creating a TDengine Source object, the data type needs to be specified as SourceRecords \<type \>| The type here is the type used to receive data from downstream operators|
| TDengineConfigParams. PROPERTYKEY_CARSET | string | The character set used by the client, with the default value being the system character set. ||
| TDengineConfigParams. PROPERTYKEY.MSSAGE_maIT_TIMEOUT | integer | Message timeout, in milliseconds, default value is 60000 ||
| TDengineConfigParams. Whether compression is enabled during the transmission process. true:  Enable, false:  Not enabled. Default is false ||
| TDengineConfigParams. Whether to enable automatic reconnection or not. true:  Enable, false:  Not enabled. Default to false||
| TDengineConfigParams. PROPERTYKEY-RECONNECT-RETR_COUNT | integer | number of automatic reconnection retries, default value 3 | only takes effect when PROPERTYKEY-INABLE AUTO-RECONNECT is true|
| TDengineConfigParams. PROPERTYKEYDISABLE_SSL_CERTVNet | boolean | Disable SSL certificate verification. true:  close, false:  Not closed. The default is false||

#### Split by time

Users can split the SQL query into multiple subtasks based on time, entering: start time, end time, split interval, time field name. The system will split and obtain data in parallel according to the set interval (time left closed and right open).

```java
SourceSplitSql splitSql = new SourceSplitSql();
splitSql.setSql("select  ts, `current`, voltage, phase, groupid, location, tbname from meters")
.setSplitType(SplitType.SPLIT_TYPE_TIMESTAMP)
.setTimestampSplitInfo(new TimestampSplitInfo(
        "2024-12-19 16:12:48.000",
        "2024-12-19 19:12:48.000",
                               "ts",
                       Duration.ofHours(1),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
        ZoneId.of("Asia/Shanghai")));
```

Splitting by Super Table TAG

Users can split the query SQL into multiple query conditions based on the TAG field of the super table, and the system will split them into subtasks corresponding to each query condition, thereby obtaining data in parallel.

```java
SourceSplitSql splitSql = new SourceSplitSql();
splitSql.setSql("select  ts, current, voltage, phase, groupid, location from meters where voltage > 100")
        .setTagList(Arrays.asList("groupid >100 and location = 'Shanghai'", 
            "groupid >50 and groupid < 100 and location = 'Guangzhou'", 
            "groupid >0 and groupid < 50 and location = 'Beijing'"))
        .setSplitType(SplitType.SPLIT_TYPE_TAG);                     
```

Classify by table

Support sharding by inputting multiple super tables or regular tables with the same table structure. The system will split them according to the method of one table, one task, and then obtain data in parallel.

```java
SourceSplitSql splitSql = new SourceSplitSql();
splitSql.setSelect("ts, current, voltage, phase, groupid, location")
        .setTableList(Arrays.asList("d1001", "d1002"))
        .setOther("order by ts limit 100")
        .setSplitType(SplitType.SPLIT_TYPE_TABLE);                    
```

Use Source connector

The query result is RowData data type example:

````java
static void testSource() throws Exception {
    Properties connProps = new Properties();
    connProps.setProperty(TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
    connProps.setProperty(TDengineConfigParams.PROPERTY_KEY_TIME_ZONE, "UTC-8");
    connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
    connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(3);

    splitSql.setSql("select  ts, `current`, voltage, phase, groupid, location, tbname from meters")
        .setSplitType(SplitType.SPLIT_TYPE_TIMESTAMP)
        .setTimestampSplitInfo(new TimestampSplitInfo(
        "2024-12-19 16:12:48.000",
        "2024-12-19 19:12:48.000",
        "ts",
        Duration.ofHours(1),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
        ZoneId.of("Asia/Shanghai")));

    TDengineSource<RowData> source = new TDengineSource<>(connProps, sql, RowData.class);
    DataStreamSource<RowData> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "tdengine-source");
    DataStream<String> resultStream = input.map((MapFunction<RowData, String>) rowData -> {
        StringBuilder sb = new StringBuilder();
        sb.append("ts: " + rowData.getTimestamp(0, 0) +
                ", current: " + rowData.getFloat(1) +
                ", voltage: " + rowData.getInt(2) +
                ", phase: " + rowData.getFloat(3) +
                ", location: " + new String(rowData.getBinary(4)));
        sb.append("\n");
        return sb.toString();
    });
    resultStream.print();
    env.execute("tdengine flink source");
   
}
````

Example of custom data type query result:

```java
void testCustomTypeSource() throws Exception {
    System.out.println("testTDengineSourceByTimeSplit start！");
    Properties connProps = new Properties();
    connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
    connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
    connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "com.taosdata.flink.entity.ResultSoureDeserialization");
    connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
    SourceSplitSql splitSql = new SourceSplitSql();
    splitSql.setSql("select  ts, `current`, voltage, phase, groupid, location, tbname from meters")
            .setSplitType(SplitType.SPLIT_TYPE_TIMESTAMP)
            //按照时间分片
            .setTimestampSplitInfo(new TimestampSplitInfo(
                    "2024-12-19 16:12:48.000",
                    "2024-12-19 19:12:48.000",
                    "ts",
                    Duration.ofHours(1),
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
                    ZoneId.of("Asia/Shanghai")));

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(3);
    TDengineSource<ResultBean> source = new TDengineSource<>(connProps, splitSql, ResultBean.class);
    DataStreamSource<ResultBean> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "tdengine-source");
    DataStream<String> resultStream = input.map((MapFunction<ResultBean, String>) rowData -> {
        StringBuilder sb = new StringBuilder();
        sb.append("ts: " + rowData.getTs() +
                ", current: " + rowData.getCurrent() +
                ", voltage: " + rowData.getVoltage() +
                ", phase: " + rowData.getPhase() +
                ", groupid: " + rowData.getGroupid() +
                ", location" + rowData.getLocation() +
                ", tbname: " + rowData.getTbname());
        sb.append("\n");
        totalVoltage.addAndGet(rowData.getVoltage());
        return sb.toString();
    });
    resultStream.print();
    env.execute("flink tdengine source");
}
```
- ResultBean is a custom inner class used to define the data type of the Source query results.
- ResultSoureDeserialization is a custom inner class that inherits Tdengine RecordDesrialization and implements convert and getProducedType methods.

### CDC Data Subscription
Flink CDC is mainly used to provide data subscription functionality, which can monitor real-time changes in TDengine database data and transmit these changes in the form of data streams to Flink for processing, while ensuring data consistency and integrity.

#### Parameter Description

| Parameter Name                            |  Type   | Parameter Description                                                                                                                                                                                                                                        | Remarks                                                                                                                                                                             |
|-------------------------------------------|:-------:|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| TDengineCdcParams.BOOTSTRAP_SERVER        | string  | IP address of the server                   |   |
| TDengineCdcParams.CONNECT-USER            | string  | username                                   |   |
| TDengineCdcParams.CONNECT-PASS            | string  | password                                   |   |
| TDengineCdcParams.POLL_INTERVAL_MS        |   int   | Pull data interval, default 500ms          |   |
| TDengineConfigParams.VALUE_DESERIALIZER   | string  | The type of data received by the operator| If the downstream operator receives data of RowData type, it only needs to be set to RowData. If the user needs to customize the type, the complete class path needs to be set here |
| TDengineCdcParams.TMQ_STACTMODE           | boolean | This parameter is used to batch push data to downstream operators. If set to True, when creating a TDengine CdcSource object, the data type needs to be specified as ConsumerRecords \<type \>                                                               | The type here is the type used to receive data from downstream operators                                                                                                            |
| TDengineCdcParams.GROUP ID                | string  | Consumption group ID, shared consumption progress within the same consumption group                                                                                                                                                                          | <br/>* * Required field * *. Maximum length: 192< Each topic can create up to 100 consumers                                                                                         
| TDengineCdcParams.AUTO-OFFSET-REET        | string  | Initial position of consumer group subscription                                                                                                                                                                                                              | early: subscribe from scratch<br/>latest: default;  Subscribe only from the latest data                                                                                             |
| TDengineCdcParams.ENABLEAUTO_CMMIT        | boolean | Whether to automatically submit, true:  Enable (for downstream stateless operators); false： Commit triggered by checkpoint                                                                                                                                   | default false                                                                                                                                                                       |
| TDengineCdcParams.AUTO_CMMIT_INTERVAL_S   | integer | The time interval for automatically submitting consumption records to consumption sites, in milliseconds                                                                                                                                                     | The default value is 5000, and this parameter takes effect when AUTO_oFFSET-REET is set to true                                                                                     |
| TDengineCdcParams.TMQ_SSSION_TIMEOUT_SS   | integer | timeout after consumer heartbeat loss, which triggers rebalancing logic. After success, the consumer will be deleted (supported from TDengine 3.3.3.0 version)                                                                                               | default value is 12000, value range [60001800000]                                                                                                                                   |
| TDengineCdcParams.TMQ_maX_POLL_INTERVAL_S | integer | The longest time interval for pulling data from a consumer poll. If this time is exceeded, the consumer will be considered offline and the rebalancing logic will be triggered. After success, the consumer will be deleted (supported from version 3.3.3.0) | The default value is 300000, [1000，INT32_MAX]                                                                                                                                       

#### Use CDC connector

The CDC connector will create consumers based on the parallelism set by the user, so the user should set the parallelism reasonably according to the resource situation.
The subscription result is RowData data type example:

```java
 void testTDengineCdc() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(3);
    env.enableCheckpointing(100, AT_LEAST_ONCE);
    env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
    Properties config = new Properties();
    config.setProperty(TDengineCdcParams.CONNECT_TYPE, "ws");
    config.setProperty(TDengineCdcParams.BOOTSTRAP_SERVERS, "localhost:6041");
    config.setProperty(TDengineCdcParams.AUTO_OFFSET_RESET, "earliest");
    config.setProperty(TDengineCdcParams.MSG_WITH_TABLE_NAME, "true");
    config.setProperty(TDengineCdcParams.AUTO_COMMIT_INTERVAL_MS, "1000");
    config.setProperty(TDengineCdcParams.GROUP_ID, "group_1");
    config.setProperty(TDengineCdcParams.ENABLE_AUTO_COMMIT, "true");
    config.setProperty(TDengineCdcParams.CONNECT_USER, "root");
    config.setProperty(TDengineCdcParams.CONNECT_PASS, "taosdata");
    config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER, "RowData");
    config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER_ENCODING, "UTF-8");
    TDengineCdcSource<RowData> tdengineSource = new TDengineCdcSource<>("topic_meters", config, RowData.class);
    DataStreamSource<RowData> input = env.fromSource(tdengineSource, WatermarkStrategy.noWatermarks(), "kafka-source");
    DataStream<String> resultStream = input.map((MapFunction<RowData, String>) rowData -> {
        StringBuilder sb = new StringBuilder();
        sb.append("tsxx: " + rowData.getTimestamp(0, 0) +
                ", current: " + rowData.getFloat(1) +
                ", voltage: " + rowData.getInt(2) +
                ", phase: " + rowData.getFloat(3) +
                ", location: " + new String(rowData.getBinary(4)));
        sb.append("\n");
        totalVoltage.addAndGet(rowData.getInt(2));
        return sb.toString();
    });
    resultStream.print();
    JobClient jobClient = env.executeAsync("Flink test cdc Example");
    Thread.sleep(8000L);
    // The task submitted by Flink UI cannot be cancle and needs to be stopped on the UI page.
    jobClient.cancel().get();
}
```

Example of custom data type query result:

````java
static void testCustomTypeCdc() throws Exception {
    System.out.println("testCustomTypeTDengineCdc start！");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(3);
    env.enableCheckpointing(100, AT_LEAST_ONCE);
    env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
    env.getCheckpointConfig().setTolerableCheckpointFailureNumber(4);
    Properties config = new Properties();
    config.setProperty(TDengineCdcParams.CONNECT_TYPE, "ws");
    config.setProperty(TDengineCdcParams.BOOTSTRAP_SERVERS, "localhost:6041");
    config.setProperty(TDengineCdcParams.AUTO_OFFSET_RESET, "earliest");
    config.setProperty(TDengineCdcParams.MSG_WITH_TABLE_NAME, "true");
    config.setProperty(TDengineCdcParams.AUTO_COMMIT_INTERVAL_MS, "1000");
    config.setProperty(TDengineCdcParams.GROUP_ID, "group_1");
    config.setProperty(TDengineCdcParams.CONNECT_USER, "root");
    config.setProperty(TDengineCdcParams.CONNECT_PASS, "taosdata");
    config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER, "com.taosdata.flink.entity.ResultDeserializer");
    config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER_ENCODING, "UTF-8");
    TDengineCdcSource<ResultBean> tdengineSource = new TDengineCdcSource<>("topic_meters", config, ResultBean.class);
    DataStreamSource<ResultBean> input = env.fromSource(tdengineSource, WatermarkStrategy.noWatermarks(), "kafka-source");
    DataStream<String> resultStream = input.map((MapFunction<ResultBean, String>) rowData -> {
        StringBuilder sb = new StringBuilder();
        sb.append("ts: " + rowData.getTs() +
                ", current: " + rowData.getCurrent() +
                ", voltage: " + rowData.getVoltage() +
                ", phase: " + rowData.getPhase() +
                ", groupid: " + rowData.getGroupid() +
                ", location" + rowData.getLocation() +
                ", tbname: " + rowData.getTbname());
        sb.append("\n");
        totalVoltage.addAndGet(rowData.getVoltage());
        return sb.toString();
    });
    resultStream.print();
    JobClient jobClient = env.executeAsync("Flink test cdc Example");
    Thread.sleep(8000L);
    jobClient.cancel().get();
}
````
### Sink

The core function of Sink is to efficiently and accurately write Flink processed data from different data sources or operators into TDengine. In this process, the efficient write mechanism possessed by TDengine played a crucial role, effectively ensuring the fast and stable storage of data.

#### Sink Properties

| Parameter Name                                          |                                                 Type                                                 | Parameter Description                                                                                                                                                               | Remarks|
|---------------------------------------------------------|:----------------------------------------------------------------------------------------------------:|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| TDengineConfigParams.PROPERTYKEYUSER                    |                                                string                                                | Login TDengine username, default value 'root'                                                                                                                                       ||
| TDengineConfigParams.PROPERTYKEY-PASSWORD               |                                                string                                                | User login password, default value 'taosdata'                                                                                                                                       ||
| TDengineConfigParams.PROPERTYKEYDBNAME                  |                                                string                                                | Database name written                                                                                                                                                               ||
| TDengineConfigParams.TD_SUPERTABLeNAME                  |                                                string                                                | Name of the super table to be written                                                                                                                                               | If the data received by the super table must have a tbname field, determine which sub table to write to|
| TDengineConfigParams.TD_TABLeNAME                       |                                                string                                                | The name of the table to be written, this parameter only needs to be set together with TD_SUPERTABLeNAME                                                                            | Used to determine which sub table or regular table to write to|
| TDengineConfigParams.TD_STACTISZE                       |                                               integer                                                | Set batch size                                                                                                                                                                      | Write when the batch quantity is reached, or a checkpoint time will also trigger writing to the database|
| TDengineConfigParams.VALUE_DESERIALIZER                 |                                                string                                                | If the downstream operator receives data of RowData type, it only needs to be set to RowData. If the user needs to customize the type, the complete class path needs to be set here |
| TDengineConfigParams.TD_STACTMODE                       |                                               boolean                                                | This parameter is used to set the reception of batch data                                                                                                                           | If set to True:< The source is TDengine Source, using SourceRecords \<type \>to create TDengine Sink object<br/>The source is TDengine CDC, using ConsumerRecords \<type \>to create TDengine Sink object | The type here is the type that receives data|
| TDengineConfigParams.TD_SOURCETYPE                      |                                                string                                                | If the data is from a source, such as source or cdc                                                                                                                                 | TDengine source is set to "tdengine_stource", TDengine cdc is set to "tdengine_cdc"|
| TDengineConfigParams.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT  |                                               integer                                                | Message timeout, in milliseconds, default value is 60000                                                                                                                            ||
| TDengineConfigParams.PROPERTY_KEY_ENABLE_COMPRESSION    |                                               boolean                                                | Whether compression is enabled during the transmission process. true:  Enable, false:  Not enabled. Default is false                                                                |         |
| TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT | integer| to enable automatic reconnection or not. true:  Enable, false:  Not enabled. Default to false                                                                                       |                                                                                                                             |
| TDengineConfigParams.PROPERTYKEY_RECONNECT_RETR_COUNT   |                                               integer                                                | number of automatic reconnection retries, default value 3                                                                                                                           | only takes effect when PROPERTYKEY-INABLE AUTO-RECONNECT is true|
| TDengineConfigParams.PROPERTYKEYDISABLE_SSL_CERTVNet    |                                               boolean                                                | Disable SSL certificate verification. true:  close, false:  Not closed. The default is false                                                                                        ||

#### Use Sink connector
Write the received RowData type data into TDengine example:
 
```java
static void testRowDataToSink() throws Exception {
    Properties connProps = new Properties();
    connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
    connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
    SourceSplitSql splitSql = getTimeSplit();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
    TDengineSource<RowData> source = new TDengineSource<>(connProps, sql, RowData.class);
    DataStreamSource<RowData> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "tdengine-source");
    Properties sinkProps = new Properties();
    sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
    sinkProps.setProperty(TDengineConfigParams.TD_SOURCE_TYPE, "tdengine_source");
    sinkProps.setProperty(TDengineConfigParams.TD_DATABASE_NAME, "power_sink");
    sinkProps.setProperty(TDengineConfigParams.TD_SUPERTABLE_NAME, "sink_meters");
    sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power_sink?user=root&password=taosdata");
    sinkProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");

    // Arrays.asList The list of target table field names needs to be consistent with the data order
    TDengineSink<RowData> sink = new TDengineSink<>(sinkProps,
            Arrays.asList("ts", "current", "voltage", "phase", "groupid", "location", "tbname"));

    input.sinkTo(sink);
    env.execute("flink tdengine source");
}
```

Write batch received RowData data into TDengine example:

```java
static void testBatchToTdSink() throws Exception {
    System.out.println("testTDengineCdcToTdSink start！");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(3);
    env.enableCheckpointing(500, CheckpointingMode.AT_LEAST_ONCE);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
    env.getCheckpointConfig().setCheckpointTimeout(5000);
    Properties config = new Properties();
    config.setProperty(TDengineCdcParams.CONNECT_TYPE, "ws");
    config.setProperty(TDengineCdcParams.BOOTSTRAP_SERVERS, "localhost:6041");
    config.setProperty(TDengineCdcParams.AUTO_OFFSET_RESET, "earliest");
    config.setProperty(TDengineCdcParams.MSG_WITH_TABLE_NAME, "true");
    config.setProperty(TDengineCdcParams.AUTO_COMMIT_INTERVAL, "1000");
    config.setProperty(TDengineCdcParams.GROUP_ID, "group_1");
    config.setProperty(TDengineCdcParams.CONNECT_USER, "root");
    config.setProperty(TDengineCdcParams.CONNECT_PASS, "taosdata");
    config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER, "RowData");
    config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER_ENCODING, "UTF-8");
    config.setProperty(TDengineCdcParams.TMQ_BATCH_MODE, "true");

    Class<ConsumerRecords<RowData>> typeClass = (Class<ConsumerRecords<RowData>>) (Class<?>) ConsumerRecords.class;
    TDengineCdcSource<ConsumerRecords<RowData>> tdengineSource = new TDengineCdcSource<>("topic_meters", config, typeClass);
    DataStreamSource<ConsumerRecords<RowData>> input = env.fromSource(tdengineSource, WatermarkStrategy.noWatermarks(), "tdengine-source");

    Properties sinkProps = new Properties();
    sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
    sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
    sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
    sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
    sinkProps.setProperty(TDengineConfigParams.TD_BATCH_MODE, "true");
    sinkProps.setProperty(TDengineConfigParams.TD_SOURCE_TYPE, "tdengine_cdc");
    sinkProps.setProperty(TDengineConfigParams.TD_DATABASE_NAME, "power_sink");
    sinkProps.setProperty(TDengineConfigParams.TD_SUPERTABLE_NAME, "sink_meters");
    sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
    sinkProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");

    TDengineSink<ConsumerRecords<RowData>> sink = new TDengineSink<>(sinkProps, Arrays.asList("ts", "current", "voltage", "phase", "location", "groupid", "tbname"));
    input.sinkTo(sink);
    JobClient jobClient = env.executeAsync("Flink test cdc Example");
    Thread.sleep(8000L);
    jobClient.cancel().get();
    System.out.println("testTDengineCdcToTdSink finish！");
}
```

### Table SQL

ETL (Extract, Transform, Load) data processing: Flink SQL with JDBC can be used to extract data from multiple different data source databases (such as TDengine, MySQL, Oracle, etc.), perform transformation operations (such as data cleaning, format conversion, associating data from different tables, etc.) in Flink, and then load the processed results into the target data source (such as TDengine, MySQL, etc.).
#### Source connector

Parameter configuration instructions:

| Parameter Name        | Type | Parameter Description | Remarks|
|-----------------------| :-----: | ------------ | ------ |
| connector             | string | connector identifier, set tdengine connector||
| td.jdbc.url           | string | URL of the connection ||
| td.jdbc.mode          | strng | connector type, set source, cdc, sink| |
| table.name            | string | Original or target table name ||
| scan.query            | string | SQL statement to retrieve data||
| sink.db.name          | string | Target database name||
| sink.superstable.name | string | Write the name of the superstable||
| sink.batch.size       | integer | batch size written||
| sink.table.name       | string | Name of the regular table or sub table written||

#### Example of using Source connector
```java
 static void testTableToSink() throws Exception {
        System.out.println("testTableToSink start！");
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);
        String tdengineSourceTableDDL = "CREATE TABLE `meters` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT," +
                " location VARBINARY," +
                " groupid INT," +
                " tbname VARBINARY" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata'," +
                "  'td.jdbc.mode' = 'source'," +
                "  'table.name' = 'meters'," +
                "  'scan.query' = 'SELECT ts, `current`, voltage, phase, location, groupid, tbname FROM `meters`'" +
                ")";


        String tdengineSinkTableDDL = "CREATE TABLE `sink_meters` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT," +
                " location VARBINARY," +
                " groupid INT," +
                " tbname VARBINARY" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'td.jdbc.mode' = 'sink'," +
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://localhost:6041/power_sink?user=root&password=taosdata'," +
                "  'sink.db.name' = 'power_sink'," +
                "  'sink.supertable.name' = 'sink_meters'" +
                ")";

        tableEnv.executeSql(tdengineSourceTableDDL);
        tableEnv.executeSql(tdengineSinkTableDDL);
        tableEnv.executeSql("INSERT INTO sink_meters SELECT ts, `current`, voltage, phase, location, groupid, tbname FROM `meters`");
    }


```

#### CDC connector
Parameter configuration instructions:

|Parameter Name | Type | Parameter Description | Remarks|
| ----------------------- | :-----: | ------------ |-------|
|Connector | string | connector identifier, set tdengine connector||
|User | string | username, default root ||
|Password | string | password, default taosdata ||
|Bootstrap. servers | string | server address ||
|Topic | string | subscribe to topic||
|Td. jdbc. mode | strng | connector type, cdc, sink| |
|Group.id | string | Consumption group ID, sharing consumption progress within the same consumption group ||
|Auto.offset.reset | string | initial position for consumer group subscription | earliest: subscribe from scratch<br/>latest: default;  Subscribe only from the latest data|
|Poll.interval_mas | integer | Pull data interval, default 500ms ||
|Sink. db. name | string | Target database name||
|Sink. superstable. name | string | Write the name of the superstable||
|Sink. batch. size | integer | batch size written||
|Sink. table. name | string | Name of the regular table or sub table written||

#### Example of using CDC connector

```java
static void testCdcTableToSink() throws Exception {
    EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(5);
    env.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);
    String tdengineSourceTableDDL = "CREATE TABLE `meters` (" +
            " ts TIMESTAMP," +
            " `current` FLOAT," +
            " voltage INT," +
            " phase FLOAT," +
            " location VARBINARY," +
            " groupid INT," +
            " tbname VARBINARY" +
            ") WITH (" +
            "  'connector' = 'tdengine-connector'," +
            "  'bootstrap.servers' = 'localhost:6041'," +
            "  'td.jdbc.mode' = 'cdc'," +
            "  'group.id' = 'group_22'," +
            "  'auto.offset.reset' = 'earliest'," +
            "  'enable.auto.commit' = 'false'," +
            "  'topic' = 'topic_meters'" +
            ")";


    String tdengineSinkTableDDL = "CREATE TABLE `sink_meters` (" +
            " ts TIMESTAMP," +
            " `current` FLOAT," +
            " voltage INT," +
            " phase FLOAT," +
            " location VARBINARY," +
            " groupid INT," +
            " tbname VARBINARY" +
            ") WITH (" +
            "  'connector' = 'tdengine-connector'," +
            "  'td.jdbc.mode' = 'cdc'," +
            "  'td.jdbc.url' = 'jdbc:TAOS-WS://localhost:6041/power_sink?user=root&password=taosdata'," +
            "  'sink.db.name' = 'power_sink'," +
            "  'sink.supertable.name' = 'sink_meters'" +
            ")";

    tableEnv.executeSql(tdengineSourceTableDDL);
    tableEnv.executeSql(tdengineSinkTableDDL);

    TableResult tableResult = tableEnv.executeSql("INSERT INTO sink_meters SELECT ts, `current`, voltage, phase, location, groupid, tbname FROM `meters`");

    Thread.sleep(5000L);
    tableResult.getJobClient().get().cancel().get();
}
```