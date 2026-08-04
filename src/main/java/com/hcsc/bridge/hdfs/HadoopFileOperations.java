package com.hcsc.bridge.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

@Component
@Profile("!local")
public class HadoopFileOperations implements HdfsFileOperations {

    private static final Logger logger = LoggerFactory.getLogger(HadoopFileOperations.class);

    private final Configuration configuration;
    private FileSystem fileSystem;

    public HadoopFileOperations(@Qualifier("hadoopConfiguration") Configuration configuration) {
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
        // HDFS delete() returns false without throwing (e.g. permission change, path
        // vanished) — surface it, or callers log "cleaned up" while the file remains.
        boolean deleted = fileSystem.delete(new Path(path), false);
        if (!deleted && fileSystem.exists(new Path(path))) {
            logger.warn("HDFS delete returned false and the path still exists: {}", path);
        }
    }

    @Override
    public String getFileChecksum(String path) throws IOException {
        return calculateLocalChecksum(path);
    }

    @Override
    public String readUtf8(String path) throws IOException {
        try (var inputStream = fileSystem.open(new Path(path));
             var out = new java.io.ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
            return out.toString(java.nio.charset.StandardCharsets.UTF_8.name());
        }
    }

    private String calculateLocalChecksum(String path) throws IOException {
        try (var inputStream = fileSystem.open(new Path(path))) {
            return com.hcsc.bridge.core.DigestUtil.sha256Hex(inputStream);
        }
    }

    @Override
    public void mkdirs(String path) throws IOException {
        fileSystem.mkdirs(new Path(path));
    }

    @Override
    public List<HdfsFileInfo> listFiles(String path) throws IOException {
        Path dir = new Path(path);
        if (!fileSystem.exists(dir)) {
            return List.of();
        }
        List<HdfsFileInfo> files = new ArrayList<>();
        for (FileStatus status : fileSystem.listStatus(dir)) {
            if (status.isFile()) {
                files.add(new HdfsFileInfo(status.getPath().toString(), status.getModificationTime()));
            }
        }
        return files;
    }
}
