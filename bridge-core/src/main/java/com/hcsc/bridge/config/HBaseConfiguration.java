package com.hcsc.bridge.config;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.annotation.PreDestroy;
import java.io.IOException;

@Configuration
@Profile("hbase")
public class HBaseConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(HBaseConfiguration.class);

    @Value("${bridge.hbase.zookeeper-quorum:localhost}")
    private String zookeeperQuorum;

    @Value("${bridge.hbase.zookeeper-port:2181}")
    private int zookeeperPort;

    @Value("${bridge.hbase.znode-parent:/hbase}")
    private String znodeParent;

    private Connection connection;

    @Bean
    public org.apache.hadoop.conf.Configuration hbaseConfiguration() {
        org.apache.hadoop.conf.Configuration config = org.apache.hadoop.hbase.HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zookeeperQuorum);
        config.setInt("hbase.zookeeper.property.clientPort", zookeeperPort);
        config.set("zookeeper.znode.parent", znodeParent);
        config.setInt("hbase.client.retries.number", 3);
        config.setInt("hbase.client.pause", 1000);
        config.setInt("hbase.rpc.timeout", 30000);
        config.setInt("hbase.client.operation.timeout", 60000);
        return config;
    }

    @Bean
    public Connection hbaseConnection(org.apache.hadoop.conf.Configuration hbaseConfig) throws IOException {
        logger.info("Creating HBase connection to ZooKeeper: {}:{}", zookeeperQuorum, zookeeperPort);
        this.connection = ConnectionFactory.createConnection(hbaseConfig);
        return connection;
    }

    @PreDestroy
    public void closeConnection() {
        if (connection != null && !connection.isClosed()) {
            try {
                connection.close();
                logger.info("HBase connection closed");
            } catch (IOException e) {
                logger.warn("Error closing HBase connection", e);
            }
        }
    }
}
