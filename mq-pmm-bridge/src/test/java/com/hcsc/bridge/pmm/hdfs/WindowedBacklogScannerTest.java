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
    @DisplayName("reports only in-flight temp files from the current and previous window; landed files are read in place")
    void reportsOnlyTempFilesInBoundedWindows() throws Exception {
        Clock clock = Clock.fixed(Instant.parse("2026-09-13T00:10:00Z"), ZoneOffset.UTC);
        WindowedPathResolver resolver = new WindowedPathResolver("/data/pmm", "", 4, "UTC", "yyyy-MM-dd", ".xml", clock);
        HdfsFileInfo landedOld = new HdfsFileInfo("/data/pmm/2026-09-13_00/a.xml", 1L);
        HdfsFileInfo inFlight = new HdfsFileInfo("/data/pmm/2026-09-13_00/b.0f3a.xml.tmp", 2L);
        HdfsFileInfo landedPrevious = new HdfsFileInfo("/data/pmm/2026-09-12_20/c.xml", 3L);
        HdfsFileInfo orphanPrevious = new HdfsFileInfo("/data/pmm/2026-09-12_20/d.77aa.xml.tmp", 4L);
        when(hdfs.listFiles("/data/pmm/2026-09-13_00")).thenReturn(List.of(landedOld, inFlight));
        when(hdfs.listFiles("/data/pmm/2026-09-12_20")).thenReturn(List.of(landedPrevious, orphanPrevious));

        List<HdfsFileInfo> files = new WindowedBacklogScanner(hdfs, resolver, 2, ".tmp").scan();

        assertThat(files).containsExactly(inFlight, orphanPrevious);
        verify(hdfs).listFiles("/data/pmm/2026-09-13_00");
        verify(hdfs).listFiles("/data/pmm/2026-09-12_20");
        verifyNoMoreInteractions(hdfs);
    }
}
