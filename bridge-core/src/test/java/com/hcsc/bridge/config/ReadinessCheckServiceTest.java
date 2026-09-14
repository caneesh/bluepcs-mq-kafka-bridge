package com.hcsc.bridge.config;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("ReadinessCheckService HA nameservice probing")
class ReadinessCheckServiceTest {

    @SuppressWarnings("unchecked")
    private ReadinessCheckService serviceWith(Configuration conf) {
        ObjectProvider<Configuration> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(conf);
        return new ReadinessCheckService(provider);
    }

    @Test
    @DisplayName("passes when one namenode behind the nameservice is reachable")
    void passesWhenOneNamenodeReachable() throws Exception {
        try (ServerSocket listening = new ServerSocket(0)) {
            Configuration conf = new Configuration(false);
            conf.set("dfs.ha.namenodes.PRDODPHA", "nn1,nn2");
            // nn1 unreachable (closed port on localhost), nn2 is our listening socket
            conf.set("dfs.namenode.rpc-address.PRDODPHA.nn1", "localhost:1");
            conf.set("dfs.namenode.rpc-address.PRDODPHA.nn2",
                    "localhost:" + listening.getLocalPort());

            ReadinessCheckService.CheckResult result =
                    serviceWith(conf).checkHaNameservice("HDFS_CONNECTION", "PRDODPHA");

            assertThat(result.getStatus()).isEqualTo(ReadinessCheckService.CheckResult.Status.PASS);
            assertThat(result.getMessage()).contains("nn2");
        }
    }

    @Test
    @DisplayName("fails with an actionable message when the nameservice has no HA mapping")
    void failsWithoutHaMapping() {
        Configuration conf = new Configuration(false);

        ReadinessCheckService.CheckResult result =
                serviceWith(conf).checkHaNameservice("HDFS_CONNECTION", "PRDODPHA");

        assertThat(result.getStatus()).isEqualTo(ReadinessCheckService.CheckResult.Status.FAIL);
        assertThat(result.getMessage()).contains("dfs.ha.namenodes.PRDODPHA");
        assertThat(result.getMessage()).contains("HADOOP_CONF_DIR");
    }

    @Test
    @DisplayName("fails when no namenode behind the nameservice is reachable")
    void failsWhenNoNamenodeReachable() {
        Configuration conf = new Configuration(false);
        conf.set("dfs.ha.namenodes.PRDODPHA", "nn1");
        conf.set("dfs.namenode.rpc-address.PRDODPHA.nn1", "localhost:1");

        ReadinessCheckService.CheckResult result =
                serviceWith(conf).checkHaNameservice("HDFS_CONNECTION", "PRDODPHA");

        assertThat(result.getStatus()).isEqualTo(ReadinessCheckService.CheckResult.Status.FAIL);
        assertThat(result.getMessage()).contains("No namenode of HA nameservice PRDODPHA");
    }

    @Test
    @DisplayName("fails cleanly when no Hadoop configuration is available")
    void failsWithoutHadoopConfiguration() {
        ReadinessCheckService.CheckResult result =
                serviceWith(null).checkHaNameservice("HDFS_CONNECTION", "PRDODPHA");

        assertThat(result.getStatus()).isEqualTo(ReadinessCheckService.CheckResult.Status.FAIL);
        assertThat(result.getMessage()).contains("no Hadoop configuration");
    }
}
