package com.hcsc.bridge.hdfs;

import com.hcsc.bridge.model.HdfsWriteResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("SafeHdfsWriter")
class SafeHdfsWriterTest {

    private static final String TEMP_SUFFIX = ".tmp";

    @Mock
    private HdfsFileOperations hdfsFileOperations;

    private SafeHdfsWriter writer;

    @BeforeEach
    void setUp() {
        writer = new SafeHdfsWriter(hdfsFileOperations, TEMP_SUFFIX);
    }

    @Nested
    @DisplayName("temp path naming")
    class TempPathNaming {

        @Test
        @DisplayName("keeps the .json.tmp shape the landing sweep matches")
        void jsonTarget() {
            String temp = writer.buildTempPath("/data/bridge/payloads/abc.json");
            assertThat(temp).startsWith("/data/bridge/payloads/abc.").endsWith(".json" + TEMP_SUFFIX);
        }

        @Test
        @DisplayName("puts the token before any other extension too")
        void xmlTarget() {
            String temp = writer.buildTempPath("/data/pmm/2026-09-13/08/abc.xml");
            assertThat(temp).startsWith("/data/pmm/2026-09-13/08/abc.").endsWith(".xml" + TEMP_SUFFIX);
            assertThat(temp).doesNotContain("..");
        }

        @Test
        @DisplayName("appends token and suffix when the target has no extension")
        void extensionlessTarget() {
            String temp = writer.buildTempPath("/data/bridge/abc");
            assertThat(temp).startsWith("/data/bridge/abc.").endsWith(TEMP_SUFFIX);
            assertThat(temp.substring("/data/bridge/abc.".length())).doesNotStartWith(".");
        }

        @Test
        @DisplayName("ignores dots in directory names and leading dots in file names")
        void dotsElsewhere() {
            assertThat(writer.buildTempPath("/data.v2/bridge/abc")).startsWith("/data.v2/bridge/abc.");
            assertThat(writer.buildTempPath("/data/bridge/.hidden")).startsWith("/data/bridge/.hidden.");
        }

        @Test
        @DisplayName("is unique per attempt")
        void uniquePerAttempt() {
            assertThat(writer.buildTempPath("/x/a.xml")).isNotEqualTo(writer.buildTempPath("/x/a.xml"));
        }
    }

    @Nested
    @DisplayName("write")
    class Write {

        @Test
        @DisplayName("creates the parent directory, writes a temp file and renames it to the target")
        void happyPath() throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            when(hdfsFileOperations.exists("/data/pmm/2026-09-13/08/abc.xml")).thenReturn(false);
            when(hdfsFileOperations.create(endsWith(".xml" + TEMP_SUFFIX))).thenReturn(out);
            when(hdfsFileOperations.getFileChecksum(anyString())).thenAnswer(inv ->
                    com.hcsc.bridge.core.DigestUtil.sha256Hex(out.toByteArray()));
            when(hdfsFileOperations.rename(anyString(), anyString())).thenReturn(true);

            HdfsWriteResult result = writer.write("/data/pmm/2026-09-13/08/abc.xml", "<r/>", "MSG-1");

            assertThat(result.isNewWrite()).isTrue();
            assertThat(result.getHdfsPath()).isEqualTo("/data/pmm/2026-09-13/08/abc.xml");
            assertThat(result.getBytesWritten()).isEqualTo(4);
            verify(hdfsFileOperations).mkdirs("/data/pmm/2026-09-13/08");
            ArgumentCaptor<String> source = ArgumentCaptor.forClass(String.class);
            verify(hdfsFileOperations).rename(source.capture(), org.mockito.ArgumentMatchers.eq("/data/pmm/2026-09-13/08/abc.xml"));
            assertThat(source.getValue()).endsWith(".xml" + TEMP_SUFFIX);
        }

        @Test
        @DisplayName("accepts an existing target only when its checksum matches")
        void existingTargetSameBytes() throws IOException {
            String checksum = com.hcsc.bridge.core.DigestUtil.sha256Hex("<r/>".getBytes());
            when(hdfsFileOperations.exists("/x/abc.xml")).thenReturn(true);
            when(hdfsFileOperations.getFileChecksum("/x/abc.xml")).thenReturn(checksum);

            HdfsWriteResult result = writer.write("/x/abc.xml", "<r/>", "MSG-1");

            assertThat(result.isAlreadyExists()).isTrue();
        }
    }
}
