package com.enterprise.bridge.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.OutputStream;

@Component
public class HadoopFileOperations implements HdfsFileOperations {

    private static final Logger logger = LoggerFactory.getLogger(HadoopFileOperations.class);

    private final Configuration configuration;
    private FileSystem fileSystem;

    public HadoopFileOperations(Configuration configuration) {
        this.configuration = configuration;
    }

    @PostConstruct
    public void init() throws IOException {
        this.fileSystem = FileSystem.get(configuration);
        logger.info("Initialized HDFS FileSystem: {}", fileSystem.getUri());
    }

    @PreDestroy
    public void close() {
        if (fileSystem != null) {
            try {
                fileSystem.close();
            } catch (IOException e) {
                logger.warn("Error closing HDFS FileSystem", e);
            }
        }
    }

    @Override
    public boolean exists(String path) throws IOException {
        return fileSystem.exists(new Path(path));
    }

    @Override
    public OutputStream create(String path) throws IOException {
        return fileSystem.create(new Path(path), true);
    }

    @Override
    public boolean rename(String sourcePath, String targetPath) throws IOException {
        return fileSystem.rename(new Path(sourcePath), new Path(targetPath));
    }

    @Override
    public void delete(String path) throws IOException {
        fileSystem.delete(new Path(path), false);
    }

    @Override
    public String getFileChecksum(String path) throws IOException {
        FileChecksum checksum = fileSystem.getFileChecksum(new Path(path));
        if (checksum == null) {
            return calculateLocalChecksum(path);
        }
        return checksum.toString();
    }

    private String calculateLocalChecksum(String path) throws IOException {
        try (var inputStream = fileSystem.open(new Path(path))) {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesRead);
            }
            return bytesToHex(digest.digest());
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 not available", e);
        }
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    @Override
    public void mkdirs(String path) throws IOException {
        fileSystem.mkdirs(new Path(path));
    }
}
