
package com.yongqing.monitor.prometheus.kafka;


import com.beust.jcommander.internal.Lists;
import com.yongqing.prometheus.core.Exporter;
import io.prometheus.client.Gauge;
import kafka.admin.AdminClient;


import kafka.coordinator.group.GroupOverview;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;
import static scala.collection.JavaConverters.asJavaCollectionConverter;
import static scala.collection.JavaConverters.mapAsJavaMapConverter;

class KafkaExporter implements Exporter {
    private final Gauge gaugeOffsetLag;
    private final Gauge gaugePartitionCount;
    private final Gauge gaugeCurrentOffset;
    private final Gauge gaugeEndOffsets;
    private final Gauge gaugeBeginningOffsets;

    private AdminClient adminClient;
    private final KafkaConsumer<String, String> consumer;
    private final Pattern groupBlacklistPattern;

    private final String kafkaHostname;
    private final int kafkaPort;
    private final String kafkaSecurityProtocol;
    private final String kafkaSslEndpointIdentificationAlgorithm;
    private final String kafkaSslKeystoreLocation;
    private final String kafkaSslKeystorePassword;
    private final String kafkaSslKeystoreKey;
    private final String kafkaSslKeyPassword;
    private final String kafkaSslTruststoreLocation;
    private final String kafkaSslTruststorePassword;
    private final String kafkaSaslJaasConfig;
    private final String kafkaSaslMechanism;
    private final String kafkaGroupId;
    private final String topic;

    public KafkaExporter(String kafkaHostname, int kafkaPort, String groupBlacklistRegexp, String kafkaSecurityProtocol, String kafkaSslEndpointIdentificationAlgorithm, String kafkaSslKeystoreLocation, String kafkaSslKeystorePassword, String kafkaSslKeystoreKey, String kafkaSslKeyPassword, String kafkaSslTruststoreLocation, String kafkaSslTruststorePassword, String kafkaSaslJaasConfig, String kafkaSaslMechanism, String kafkaGroupId, String topic) {
        this.kafkaHostname = kafkaHostname;
        this.kafkaPort = kafkaPort;
        this.kafkaSecurityProtocol = kafkaSecurityProtocol;
        this.kafkaSslEndpointIdentificationAlgorithm = kafkaSslEndpointIdentificationAlgorithm;
        this.kafkaSslKeystoreLocation = kafkaSslKeystoreLocation;
        this.kafkaSslKeystorePassword = kafkaSslKeystorePassword;
        this.kafkaSslKeystoreKey = kafkaSslKeystoreKey;
        this.kafkaSslKeyPassword = kafkaSslKeyPassword;
        this.kafkaSslTruststoreLocation = kafkaSslTruststoreLocation;
        this.kafkaSslTruststorePassword = kafkaSslTruststorePassword;
        this.kafkaSaslJaasConfig = kafkaSaslJaasConfig;
        this.kafkaSaslMechanism = kafkaSaslMechanism;
        this.kafkaGroupId = kafkaGroupId;
        this.topic = topic;
        this.adminClient = createAdminClient(kafkaHostname, kafkaPort, kafkaSecurityProtocol, kafkaSslEndpointIdentificationAlgorithm, kafkaSslKeystoreLocation, kafkaSslKeystorePassword, kafkaSslKeystoreKey, kafkaSslKeyPassword, kafkaSslTruststoreLocation, kafkaSslTruststorePassword, kafkaSaslJaasConfig, kafkaSaslMechanism);
        this.consumer = createNewConsumer(kafkaHostname, kafkaPort, kafkaSecurityProtocol, kafkaSslEndpointIdentificationAlgorithm, kafkaSslKeystoreLocation, kafkaSslKeystorePassword, kafkaSslKeystoreKey, kafkaSslKeyPassword, kafkaSslTruststoreLocation, kafkaSslTruststorePassword, kafkaSaslJaasConfig, kafkaSaslMechanism, kafkaGroupId);
        this.groupBlacklistPattern = Pattern.compile(groupBlacklistRegexp);
        this.gaugePartitionCount = Gauge.build()
                .name("kafka_broker_consumer_group_partition_count")
                .help("kafka topic partition count")
                .labelNames("group_id", "topic")
                .register();
        this.gaugeOffsetLag = Gauge.build()
                .name("kafka_broker_consumer_group_offset_lag")
                .help("Offset lag of a topic/partition")
                .labelNames("group_id", "partition", "topic")
                .register();

        this.gaugeCurrentOffset = Gauge.build()
                .name("kafka_broker_consumer_group_current_offset")
                .help("Current consumed offset of a topic/partition")
                .labelNames("group_id", "partition", "topic")
                .register();
        this.gaugeEndOffsets = Gauge.build()
                .name("kafka_broker_consumer_group_end_offset")
                .help("end Offset  of a topic/partition")
                .labelNames("group_id", "partition", "topic")
                .register();

        this.gaugeBeginningOffsets = Gauge.build()
                .name("kafka_broker_consumer_group_begin_offset")
                .help("begin offset of a topic/partition")
                .labelNames("group_id", "partition", "topic")
                .register();
    }


    private AdminClient createAdminClient(String kafkaHostname, int kafkaPort, String kafkaSecurityProtocol, String kafkaSslEndpointIdentificationAlgorithm, String kafkaSslKeystoreLocation, String kafkaSslKeystorePassword, String kafkaSslKeystoreKey, String kafkaSslKeyPassword, String kafkaSslTruststoreLocation, String kafkaSslTruststorePassword, String kafkaSaslJaasConfig, String kafkaSaslMechanism) {

        Properties props = new Properties();
        if (null != kafkaSaslJaasConfig) {
            props.put("sasl.jaas.config", kafkaSaslJaasConfig);
        }
        if (null != kafkaSaslMechanism) {
            props.put("sasl.mechanism", kafkaSaslMechanism);
        }
        if (null != kafkaSecurityProtocol) {
            props.put("security.protocol", kafkaSecurityProtocol);
        }
        if (null != kafkaSslEndpointIdentificationAlgorithm) {
            props.put("ssl.endpoint.identification.algorithm", kafkaSslEndpointIdentificationAlgorithm);
        }
        if (null != kafkaSslKeystoreLocation) {
            props.put("ssl.keystore.location", kafkaSslKeystoreLocation);
        }
        if (null != kafkaSslKeystorePassword) {
            props.put("ssl.keystore.password", kafkaSslKeystorePassword);
        }
        if (null != kafkaSslKeystoreKey) {
            props.put("ssl.keystore.key", kafkaSslKeystoreKey);
        }
        if (null != kafkaSslKeyPassword) {
            props.put("ssl.key.password", kafkaSslKeyPassword);
        }
        if (null != kafkaSslTruststoreLocation) {
            props.put("ssl.truststore.location", kafkaSslTruststoreLocation);
        }
        if (null != kafkaSslTruststorePassword) {
            props.put("ssl.truststore.password", kafkaSslTruststorePassword);
        }
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostname + ":" + kafkaPort);

        return AdminClient.create(props);
    }

    synchronized void updateMetricsByConsumerAndTopic() {
        updateMetrics(this.topic);
    }

    private synchronized void updateMetrics(String topic) {
        try {
//            System.out.println("=======" + topic);
            List<PartitionInfo> partitionInfoList = this.consumer.partitionsFor(topic);
            gaugePartitionCount.labels(this.kafkaGroupId, topic).set(partitionInfoList.size());
            partitionInfoList.forEach(part -> {
                TopicPartition topicPartition = new TopicPartition(part.topic(), part.partition());
                OffsetAndMetadata offsetAndMetadata = this.consumer.committed(topicPartition);
                Long endOffsets = this.consumer.endOffsets(Lists.newArrayList(topicPartition)).get(topicPartition);
                Long beginningOffsets = this.consumer.beginningOffsets(Lists.newArrayList(topicPartition)).get(topicPartition);
                String partition = String.valueOf(part.partition());
                if (null != offsetAndMetadata) {
                    Long currentOffset = this.consumer.committed(topicPartition).offset();
                    Long lag = endOffsets - currentOffset;
                    gaugeOffsetLag.labels(this.kafkaGroupId, partition, part.topic()).set(lag);
                    gaugeCurrentOffset.labels(this.kafkaGroupId, partition, part.topic()).set(currentOffset);
                }
                gaugeEndOffsets.labels(this.kafkaGroupId, partition, part.topic()).set(endOffsets);
                gaugeBeginningOffsets.labels(this.kafkaGroupId, partition, part.topic()).set(beginningOffsets);
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    synchronized void updateMetricsByConsumer() {
        try {
            this.consumer.listTopics().forEach((k, v) -> {
                updateMetrics(k);
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

     public synchronized void updateMetrics() {
        try {
            Collection<GroupOverview> groupOverviews = asJavaCollectionConverter(adminClient.listAllConsumerGroupsFlattened()).asJavaCollection();
            List<String> groups = groupOverviews.stream()
                    .map(GroupOverview::groupId)
                    .filter(g -> !groupBlacklistPattern.matcher(g).matches())
                    .collect(toList());
            groups.forEach(group -> {
                Map<TopicPartition, Object> offsets = mapAsJavaMapConverter(adminClient.listGroupOffsets(group)).asJava();
                offsets.forEach((k, v) -> {
                    TopicPartition topicPartition = new TopicPartition(k.topic(), k.partition());
                    Long currentOffset = new Long(v.toString());
                    Long lag = getLogEndOffset(topicPartition) - currentOffset;
                    String partition = String.valueOf(k.partition());
                    gaugeOffsetLag.labels(group, partition, k.topic()).set(lag);
                    gaugeCurrentOffset.labels(group, partition, k.topic()).set(currentOffset);
                });
            });

        } catch (java.lang.RuntimeException ex) {
            ex.printStackTrace();
            this.adminClient = createAdminClient(this.kafkaHostname, this.kafkaPort, this.kafkaSecurityProtocol, this.kafkaSslEndpointIdentificationAlgorithm, this.kafkaSslKeystoreLocation, this.kafkaSslKeystorePassword, this.kafkaSslKeystoreKey, this.kafkaSslKeyPassword, this.kafkaSslTruststoreLocation, this.kafkaSslTruststorePassword, this.kafkaSaslJaasConfig, this.kafkaSaslMechanism);
        }
    }

    private long getLogEndOffset(TopicPartition topicPartition) {
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seekToEnd(Arrays.asList(topicPartition));
        return consumer.position(topicPartition);
    }

    private KafkaConsumer<String, String> createNewConsumer(String kafkaHost, int kafkaPort, String kafkaSecurityProtocol, String kafkaSslEndpointIdentificationAlgorithm, String kafkaSslKeystoreLocation, String kafkaSslKeystorePassword, String kafkaSslKeystoreKey, String kafkaSslKeyPassword, String kafkaSslTruststoreLocation, String kafkaSslTruststorePassword, String kafkaSaslJaasConfig, String kafkaSaslMechanism, String kafkaGroupId) {
        Properties properties = new Properties();
        if (null != kafkaGroupId) {
            properties.put("group.id", kafkaGroupId);
        }
        if (null != kafkaSaslJaasConfig) {
            properties.put("sasl.jaas.config", kafkaSaslJaasConfig);
        }
        if (null != kafkaSaslMechanism) {
            properties.put("sasl.mechanism", kafkaSaslMechanism);
        }
        if (null != kafkaSecurityProtocol) {
            properties.put("security.protocol", kafkaSecurityProtocol);
        }
        if (null != kafkaSslEndpointIdentificationAlgorithm) {
            properties.put("ssl.endpoint.identification.algorithm", kafkaSslEndpointIdentificationAlgorithm);
        }
        if (null != kafkaSslKeystoreLocation) {
            properties.put("ssl.keystore.location", kafkaSslKeystoreLocation);
        }
        if (null != kafkaSslKeystorePassword) {
            properties.put("ssl.keystore.password", kafkaSslKeystorePassword);
        }
        if (null != kafkaSslKeystoreKey) {
            properties.put("ssl.keystore.key", kafkaSslKeystoreKey);
        }
        if (null != kafkaSslKeyPassword) {
            properties.put("ssl.key.password", kafkaSslKeyPassword);
        }
        if (null != kafkaSslTruststoreLocation) {
            properties.put("ssl.truststore.location", kafkaSslTruststoreLocation);
        }
        if (null != kafkaSslTruststorePassword) {
            properties.put("ssl.truststore.password", kafkaSslTruststorePassword);
        }
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost + ":" + kafkaPort);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(properties);
    }

}
