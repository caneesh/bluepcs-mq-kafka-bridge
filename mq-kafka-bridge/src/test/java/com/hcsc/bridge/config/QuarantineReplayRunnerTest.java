package com.hcsc.bridge.config;

import com.hcsc.bridge.hdfs.HdfsFileOperations;
import com.hcsc.bridge.hdfs.HdfsFileOperations.HdfsFileInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("QuarantineReplayRunner")
class QuarantineReplayRunnerTest {

    private static final String ERROR_DIR = "/data/bridge/payloads/errors";
    private static final String QFILE = ERROR_DIR + "/event-1.json";
    private static final String PAYLOAD = "{\"planNotification\":{\"broken\":true}}";

    private ApplicationContext applicationContext;
    private Environment environment;
    private HdfsFileOperations hdfs;
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private MessageProducer producer;
    private TextMessage textMessage;

    @SuppressWarnings("unchecked")
    private final ObjectProvider<ConnectionFactory> connectionFactoryProvider = mock(ObjectProvider.class);
    @SuppressWarnings("unchecked")
    private final ObjectProvider<HdfsFileOperations> hdfsProvider = mock(ObjectProvider.class);

    @BeforeEach
    void setUp() throws Exception {
        applicationContext = mock(ApplicationContext.class);
        environment = mock(Environment.class);
        when(environment.getActiveProfiles()).thenReturn(new String[]{"test"});

        hdfs = mock(HdfsFileOperations.class);
        connectionFactory = mock(ConnectionFactory.class);
        connection = mock(Connection.class);
        session = mock(Session.class);
        producer = mock(MessageProducer.class);
        textMessage = mock(TextMessage.class);

        when(hdfsProvider.getObject()).thenReturn(hdfs);
        when(connectionFactoryProvider.getObject()).thenReturn(connectionFactory);
        when(connectionFactory.createConnection()).thenReturn(connection);
        when(connection.createSession(false, Session.AUTO_ACKNOWLEDGE)).thenReturn(session);
        when(session.createProducer(any())).thenReturn(producer);
        when(session.createTextMessage(anyString())).thenReturn(textMessage);
        when(textMessage.getJMSMessageID()).thenReturn("ID:NEW-MSG-1");
    }

    private QuarantineReplayRunner newRunner(String replayArg) throws Exception {
        QuarantineReplayRunner runner = new QuarantineReplayRunner(
                applicationContext, environment, connectionFactoryProvider, hdfsProvider);
        setField(runner, "replayArg", replayArg);
        setField(runner, "mqQueue", "BRIDGE.INPUT.QUEUE");
        setField(runner, "basePath", "/data/bridge/payloads");
        setField(runner, "errorPath", "");
        return runner;
    }

    private static void setField(QuarantineReplayRunner runner, String name, Object value) throws Exception {
        Field field = QuarantineReplayRunner.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(runner, value);
    }

    @Nested
    @DisplayName("replaying files")
    class ReplayFiles {

        @Test
        @DisplayName("reads the file, re-puts it verbatim, and moves it to replayed/")
        void replaysAndMovesFile() throws Exception {
            when(hdfs.exists(QFILE)).thenReturn(true);
            when(hdfs.readUtf8(QFILE)).thenReturn(PAYLOAD);
            when(hdfs.rename(QFILE, ERROR_DIR + "/replayed/event-1.json")).thenReturn(true);

            int exitCode = newRunner(QFILE).runReplay();

            assertEquals(QuarantineReplayRunner.EXIT_OK, exitCode);
            // Verbatim payload, sent as a new message
            verify(session).createTextMessage(PAYLOAD);
            verify(producer).send(textMessage);
            // Moved out of the quarantine dir so a re-run cannot double-replay
            verify(hdfs).mkdirs(ERROR_DIR + "/replayed");
            verify(hdfs).rename(QFILE, ERROR_DIR + "/replayed/event-1.json");
        }

        @Test
        @DisplayName("a missing file fails without sending anything")
        void missingFileFails() throws Exception {
            when(hdfs.exists(QFILE)).thenReturn(false);

            int exitCode = newRunner(QFILE).runReplay();

            assertEquals(QuarantineReplayRunner.EXIT_FAILED, exitCode);
            verify(producer, never()).send(any(javax.jms.Message.class));
        }

        @Test
        @DisplayName("a replayed file that cannot be moved is reported as a failure")
        void unmovableFileIsFailure() throws Exception {
            when(hdfs.exists(QFILE)).thenReturn(true);
            when(hdfs.readUtf8(QFILE)).thenReturn(PAYLOAD);
            when(hdfs.rename(anyString(), anyString())).thenReturn(false);

            int exitCode = newRunner(QFILE).runReplay();

            // The send DID happen, but exit 1 forces the operator to look: a re-run
            // would replay the still-in-place file a second time.
            assertEquals(QuarantineReplayRunner.EXIT_FAILED, exitCode);
            verify(producer).send(textMessage);
        }

        @Test
        @DisplayName("one bad file does not stop the others (partial failure, exit 1)")
        void partialFailureReplaysTheRest() throws Exception {
            String second = ERROR_DIR + "/event-2.json";
            when(hdfs.exists(QFILE)).thenReturn(false);
            when(hdfs.exists(second)).thenReturn(true);
            when(hdfs.readUtf8(second)).thenReturn(PAYLOAD);
            when(hdfs.rename(second, ERROR_DIR + "/replayed/event-2.json")).thenReturn(true);

            int exitCode = newRunner(QFILE + "," + second).runReplay();

            assertEquals(QuarantineReplayRunner.EXIT_FAILED, exitCode);
            verify(producer).send(textMessage);
        }

        @Test
        @DisplayName("MQ connection failure fails cleanly")
        void mqSetupFailureFails() throws Exception {
            when(connectionFactory.createConnection())
                    .thenThrow(new javax.jms.JMSException("QM unreachable"));

            int exitCode = newRunner(QFILE).runReplay();

            assertEquals(QuarantineReplayRunner.EXIT_FAILED, exitCode);
        }
    }

    @Nested
    @DisplayName("list mode")
    class ListMode {

        @Test
        @DisplayName("lists the quarantine directory and exits 0")
        void listsQuarantineDir() throws Exception {
            when(hdfs.listFiles(ERROR_DIR)).thenReturn(Collections.singletonList(
                    new HdfsFileInfo(QFILE, 1720000000000L)));

            int exitCode = newRunner("list").runReplay();

            assertEquals(QuarantineReplayRunner.EXIT_OK, exitCode);
            verify(hdfs).listFiles(ERROR_DIR);
        }

        @Test
        @DisplayName("a listing failure exits 1")
        void listFailureFails() throws Exception {
            when(hdfs.listFiles(anyString())).thenThrow(new IOException("kerberos"));

            int exitCode = newRunner("list").runReplay();

            assertEquals(QuarantineReplayRunner.EXIT_FAILED, exitCode);
        }
    }
}
