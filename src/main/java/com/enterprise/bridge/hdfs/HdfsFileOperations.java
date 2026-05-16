package com.enterprise.bridge.hdfs;

import java.io.IOException;
import java.io.OutputStream;

public interface HdfsFileOperations {

    boolean exists(String path) throws IOException;

    OutputStream create(String path) throws IOException;

    boolean rename(String sourcePath, String targetPath) throws IOException;

    void delete(String path) throws IOException;

    String getFileChecksum(String path) throws IOException;

    void mkdirs(String path) throws IOException;
}
