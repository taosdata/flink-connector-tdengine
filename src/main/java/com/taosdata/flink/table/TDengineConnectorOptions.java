/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.taosdata.flink.table;

import com.taosdata.jdbc.TSDBDriver;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.factories.FactoryUtil;

/** Options for the Kafka connector. */
@PublicEvolving
public class TDengineConnectorOptions {

    // --------------------------------------------------------------------------------------------
    // Format options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> TD_JDBC_URL =
            ConfigOptions.key("td.jdbc.url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Url address or hostname of the database server.");

    public static final ConfigOption<String> TD_JDBC_MODE =
            ConfigOptions.key("td.jdbc.mode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Url address or hostname of the database server.");

    public static final ConfigOption<String> TD_BATCH_MODE =
            ConfigOptions.key("td.batch.mode")
                    .stringType()
                    .defaultValue("false")
                    .withDescription("Url address or hostname of the database server.");

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key(TSDBDriver.PROPERTY_KEY_HOST)
                    .stringType()
                    .defaultValue("localhost")
                    .withDescription("IP address or hostname of the MySQL database server.");

    public static final ConfigOption<String> PORT =
            ConfigOptions.key(TSDBDriver.PROPERTY_KEY_PORT)
                    .stringType()
                    .defaultValue("6041")
                    .withDescription("Integer port number of the MySQL database server.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key(TSDBDriver.PROPERTY_KEY_USER)
                    .stringType()
                    .defaultValue("root")
                    .withDescription(
                            "Name of the MySQL database to use when connecting to the MySQL database server.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key(TSDBDriver.PROPERTY_KEY_PASSWORD)
                    .stringType()
                    .defaultValue("taosdata")
                    .withDescription(
                            "Password to use when connecting to the MySQL database server.");

    public static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key(TSDBDriver.PROPERTY_KEY_DBNAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Database name of the MySQL server to monitor.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name of the database to monitor.");

    public static final ConfigOption<String> SERVER_TIME_ZONE =
            ConfigOptions.key(TSDBDriver.PROPERTY_KEY_TIME_ZONE)
                    .stringType()
                    .defaultValue("UTC-8")
                    .withDescription(
                            "The session time zone in database server. If not set, then "
                                    + "ZoneId.systemDefault() is used to determine the server time zone.");

    public static final ConfigOption<String> CHARSET = ConfigOptions
            .key(TSDBDriver.PROPERTY_KEY_CHARSET)
            .stringType()
            .defaultValue("UTF-8")
            .withDescription("tdengine database charset");

    public static final ConfigOption<String> LOCALE = ConfigOptions
            .key(TSDBDriver.PROPERTY_KEY_LOCALE)
            .stringType()
            .defaultValue("en_US.UTF-8")
            .withDescription("tdengine database local");


    public static final ConfigOption<String> ENABLE_AUTO_RECONNECT = ConfigOptions
            .key(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT)
            .stringType()
            .defaultValue("true")
            .withDescription("tdengine database enable auto reconnect");

    public static final ConfigOption<String> SCAN_QUERY =
            ConfigOptions.key("scan.query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "query sql.");


    // --------------------------------------------------------------------------------------------
    // Tmq specific options
    // --------------------------------------------------------------------------------------------
    public static final ConfigOption<String> BOOTSTRAP_SERVERS =
            ConfigOptions.key("bootstrap.servers")
                    .stringType()
                    .defaultValue("192.168.1.98:6041")
                    .withDescription(
                            "cdc bootstrap servers address");

    public static final ConfigOption<String> CONNECT_URL =
            ConfigOptions.key("url")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "cdc connect url address");

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Topic name to read data from when the table is used as source. It also supports topic list for source by separating topic by semicolon like 'topic-1;topic-2'. Note, only one of 'topic-pattern' and 'topic' can be specified for sources. "
                                    + "When the table is used as sink, the topic name is the topic to write data. It also supports topic list for sinks. The provided topic-list is treated as a allow list of valid values for the `topic` metadata column. If  a list is provided, for sink table, 'topic' metadata column is writable and must be specified.");

    public static final ConfigOption<String> GROUP_ID =
            ConfigOptions.key("group.id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Initial position of the consumer group subscription. Maximum length: 192. Each topic can have up to 100 consumer groups");

    public static final ConfigOption<String> CLIENT_ID =
            ConfigOptions.key("client.id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Client ID, Maximum length: 192.");


    public static final ConfigOption<String> TMQ_POLL_INTERVAL_MS =
            ConfigOptions.key("poll.interval_ms")
                    .stringType()
                    .defaultValue("root")
                    .withDescription(
                            "user name");

    public static final ConfigOption<String> AUTO_OFFSET_RESET =
            ConfigOptions.key("auto.offset.reset")
                    .stringType()
                    .defaultValue("latest")
                    .withDescription(
                            "Initial position of the consumer group subscription.");


    public static final ConfigOption<String> ENABLE_AUTO_COMMIT =
            ConfigOptions.key("enable.auto.commit")
                    .stringType()
                    .defaultValue("false")
                    .withDescription(
                            "Whether to enable automatic consumption point submission, " +
                                    "'true: automatic submission, client application does not need to commit;" +
                                    " false: client application needs to commit manually.");

    public static final ConfigOption<String> VALUE_DESERIALIZER_ENCODING =
            ConfigOptions.key("value.deserializer.encoding")
                    .stringType()
                    .defaultValue("UTF-8")
                    .withDescription(
                            "value.deserializer.encoding default utf8.");

    public static final ConfigOption<String> AUTO_COMMIT_INTERVAL =
            ConfigOptions.key("auto.commit.interval.ms")
                    .stringType()
                    .defaultValue("5000")
                    .withDescription(
                            "Time interval for automatically submitting consumption records, in milliseconds.");

    // sink config options
    public static final ConfigOption<String> SINK_SUPERTABLE_NAME = ConfigOptions
            .key("sink.supertable.name")
            .stringType()
            .defaultValue("")
            .withDescription("tdengine supertable name");

    public static final ConfigOption<String> SINK_TABLE_NAME = ConfigOptions
            .key("sink.table.name")
            .stringType()
            .defaultValue("")
            .withDescription("tdengine subtable or normal name");

    public static final ConfigOption<String> SINK_DBNAME_NAME = ConfigOptions
            .key("sink.db.name")
            .stringType()
            .defaultValue("")
            .withDescription("tdengine database name");
    public static final ConfigOption<Integer> SINK_BATCH_SIZE = ConfigOptions
            .key("sink.batch.size")
            .intType()
            .defaultValue(500)
            .withDescription("tdengine database name");



    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

}