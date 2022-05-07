
package com.yongqing.monitor.prometheus.kafka;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.yongqing.prometheus.core.ExposePrometheusMetricsServer;
import io.prometheus.client.exporter.MetricsServlet;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class Main {
    @Parameter(names = "--kafka-topic", description = "topic", required = false)
    public String kafkaTopic;
    @Parameter(names = "--kafka-group-id", description = "group.id", required = false)
    public String kafkaGroupId;
    @Parameter(names = "--kafka-sasl-jaas.config", description = "sasl.jaas.config", required = false)
    public String kafkaSaslJaasConfig;
    @Parameter(names = "--kafka-sasl-mechanism", description = "sasl.mechanism", required = false)
    public String kafkaSaslMechanism;
    @Parameter(names = "--kafka-security-protocol", description = "security.protocol", required = false)
    public String kafkaSecurityProtocol;
    @Parameter(names = "--kafka-ssl-endpoint-identification-algorithm", description = "ssl.endpoint.identification.algorithm", required = false)
    public String kafkaSslEndpointIdentificationAlgorithm;
    @Parameter(names = "--kafka-ssl-keystore-location", description = "ssl.keystore.location", required = false)
    public String kafkaSslKeystoreLocation;
    @Parameter(names = "--kafka-ssl-keystore-password", description = "ssl.keystore.password", required = false)
    public String kafkaSslKeystorePassword;

    @Parameter(names = "--kafka-ssl-keystore-key", description = "ssl.keystore.key", required = false)
    public String kafkaSslKeystoreKey;
    @Parameter(names = "--kafka-ssl.key.password", description = "ssl.key.password", required = false)
    public String kafkaSslKeyPassword;

    @Parameter(names = "--kafka-ssl-truststore-location", description = "ssl.truststore.location", required = false)
    public String kafkaSslTruststoreLocation;

    @Parameter(names = "--kafka-ssl-truststore-password", description = "ssl.truststore.password", required = false)
    public String kafkaSslTruststorePassword;

    @Parameter(names = "--kafka-host", description = "Kafka hostname", required = true)
    public String kafkaHostname;

    @Parameter(names = "--kafka-port", description = "Kafka port")
    public int kafkaPort = 9092;

    @Parameter(names = "--port", description = "Exporter port")
    public int port = 7979;

    @Parameter(names = "--group-blacklist-regexp", description = "Consumer group blacklist regexp")
    public String groupBlacklistRegexp = "console-consumer.*";

    @Parameter(names = "--scrape-period", description = "Scrape period")
    public int scrapePeriod = 1;

    @Parameter(names = "--scrape-period-unit", description = "Scrape period timeunit")
    public TimeUnit scrapePeriodUnit = TimeUnit.SECONDS;

    @Parameter(names = "--help", help = true)
    private boolean help = false;

    public static void main(String... args) throws Exception {
        Main main = new Main();
        JCommander jcommander = JCommander.newBuilder()
                .addObject(main)
                .build();

        jcommander.parse(args);

        if (main.help) {
            jcommander.usage();
        } else {
            KafkaExporter kafkaExporter = new KafkaExporter(main.kafkaHostname, main.kafkaPort, main.groupBlacklistRegexp, main.kafkaSecurityProtocol, main.kafkaSslEndpointIdentificationAlgorithm, main.kafkaSslKeystoreLocation, main.kafkaSslKeystorePassword, main.kafkaSslKeystoreKey, main.kafkaSslKeyPassword, main.kafkaSslTruststoreLocation, main.kafkaSslTruststorePassword, main.kafkaSaslJaasConfig, main.kafkaSaslMechanism, main.kafkaGroupId, main.kafkaTopic);
            new Timer().scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    if (null != main.kafkaGroupId && null != main.kafkaTopic) {
                        kafkaExporter.updateMetricsByConsumerAndTopic();
                    } else if (null != main.kafkaGroupId && null == main.kafkaTopic) {
                        kafkaExporter.updateMetricsByConsumer();
                    } else {
                        kafkaExporter.updateMetricsByConsumer();
                    }
                }
            }, 0, main.scrapePeriodUnit.toMillis(main.scrapePeriod));

            ExposePrometheusMetricsServer prometheusMetricServlet = new ExposePrometheusMetricsServer(main.port, new MetricsServlet());
            prometheusMetricServlet.start();
        }
    }

}
