package com.hcsc.bridge.recovery;

public enum RecoveryAction {
    RETRY_ENRICHMENT,
    RETRY_HDFS_WRITE,
    RETRY_KAFKA_PUBLISH,
    SKIP,
    MANUAL_INTERVENTION,
    NO_ACTION
}
