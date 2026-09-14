package com.hcsc.bridge.pmm.hdfs;

import com.hcsc.bridge.hdfs.HdfsFileOperations;
import com.hcsc.bridge.hdfs.HdfsFileOperations.HdfsFileInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("WindowedBacklogScanner")
class WindowedBacklogScannerTest {

    @Mock
    private HdfsFileOperations hdfs;

    @Test
    @DisplayName("lists only the current and previous window directories")
    void scansBoundedWindows() throws Exception {
        Clock clock = Clock.fixed(Instant.parse("2026-09-13T00:10:00Z"), ZoneOffset.UTC);
        WindowedPathResolver resolver = new WindowedPathResolver("/data/pmm", "", 4, "UTC", "yyyy-MM-dd", ".xml", clock);
        HdfsFileInfo a = new HdfsFileInfo("/data/pmm/2026-09-13/00/a.xml", 1L);
        HdfsFileInfo b = new HdfsFileInfo("/data/pmm/2026-09-12/20/b.xml", 2L);
        when(hdfs.listFiles("/data/pmm/2026-09-13/00")).thenReturn(List.of(a));
        when(hdfs.listFiles("/data/pmm/2026-09-12/20")).thenReturn(List.of(b));

        List<HdfsFileInfo> files = new WindowedBacklogScanner(hdfs, resolver, 2).scan();

        assertThat(files).containsExactly(a, b);
        verify(hdfs).listFiles("/data/pmm/2026-09-13/00");
        verify(hdfs).listFiles("/data/pmm/2026-09-12/20");
        verifyNoMoreInteractions(hdfs);
    }
}
