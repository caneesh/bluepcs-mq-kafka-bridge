package com.hcsc.bridge.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;

public interface HdfsFileOperations {

    boolean exists(String path) throws IOException;

    OutputStream create(String path) throws IOException;

    boolean rename(String sourcePath, String targetPath) throws IOException;

    void delete(String path) throws IOException;

    String getFileChecksum(String path) throws IOException;

    void mkdirs(String path) throws IOException;

    /**
     * Lists the regular files directly under {@code path} (non-recursive). Returns an
     * empty list when the directory does not exist — callers such as the backlog
     * monitor treat a missing landing directory as "nothing pending".
     */
    List<HdfsFileInfo> listFiles(String path) throws IOException;

    /** Name and modification time of a file returned by {@link #listFiles}. */
    final class HdfsFileInfo {
        private final String path;
        private final long modificationTimeMillis;

        public HdfsFileInfo(String path, long modificationTimeMillis) {
            this.path = Objects.requireNonNull(path, "path must not be null");
            this.modificationTimeMillis = modificationTimeMillis;
        }

        public String getPath() {
            return path;
        }

        public long getModificationTimeMillis() {
            return modificationTimeMillis;
        }

        @Override
        public String toString() {
            return path + " (mtime=" + modificationTimeMillis + ")";
        }
    }
}
