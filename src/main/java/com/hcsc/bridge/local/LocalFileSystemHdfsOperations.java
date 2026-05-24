package com.hcsc.bridge.local;

import com.hcsc.bridge.hdfs.HdfsFileOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Component
@Profile("local")
public class LocalFileSystemHdfsOperations implements HdfsFileOperations {

    private static final Logger logger = LoggerFactory.getLogger(LocalFileSystemHdfsOperations.class);

    private final Path baseDir;

    public LocalFileSystemHdfsOperations(
            @Value("${bridge.hdfs.local-base-path:./data/hdfs}") String basePath) {
        this.baseDir = Paths.get(basePath);
    }

    @PostConstruct
    public void init() throws IOException {
        Files.createDirectories(baseDir);
        logger.info("Initialized local HDFS simulation at: {}", baseDir.toAbsolutePath());
    }

    @Override
    public boolean exists(String path) throws IOException {
        Path fullPath = resolvePath(path);
        return Files.exists(fullPath);
    }

    @Override
    public OutputStream create(String path) throws IOException {
        Path fullPath = resolvePath(path);
        Files.createDirectories(fullPath.getParent());
        logger.debug("Creating local file: {}", fullPath);
        return new FileOutputStream(fullPath.toFile());
    }

    @Override
    public boolean rename(String sourcePath, String targetPath) throws IOException {
        Path source = resolvePath(sourcePath);
        Path target = resolvePath(targetPath);

        if (!Files.exists(source)) {
            return false;
        }

        if (Files.exists(target)) {
            return false;
        }

        Files.createDirectories(target.getParent());
        Files.move(source, target);
        logger.debug("Renamed {} to {}", source, target);
        return true;
    }

    @Override
    public void delete(String path) throws IOException {
        Path fullPath = resolvePath(path);
        if (Files.exists(fullPath)) {
            Files.delete(fullPath);
            logger.debug("Deleted local file: {}", fullPath);
        }
    }

    @Override
    public String getFileChecksum(String path) throws IOException {
        Path fullPath = resolvePath(path);
        if (!Files.exists(fullPath)) {
            throw new IOException("File not found: " + path);
        }

        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (FileInputStream fis = new FileInputStream(fullPath.toFile())) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = fis.read(buffer)) != -1) {
                    md.update(buffer, 0, bytesRead);
                }
            }
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 algorithm not available", e);
        }
    }

    @Override
    public void mkdirs(String path) throws IOException {
        Path fullPath = resolvePath(path);
        Files.createDirectories(fullPath);
    }

    private Path resolvePath(String path) {
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return baseDir.resolve(path);
    }
}
