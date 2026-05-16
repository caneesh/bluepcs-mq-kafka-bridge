package com.enterprise.bridge.config;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

@org.springframework.context.annotation.Configuration
public class HdfsConfiguration {

    @Value("${bridge.hdfs.namenode:hdfs://localhost:9000}")
    private String namenode;

    @Value("${bridge.hdfs.replication:3}")
    private int replication;

    @Bean
    public Configuration hadoopConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", namenode);
        configuration.setInt("dfs.replication", replication);
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        return configuration;
    }
}
