package utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.JobClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


/**
 * Created by lei on 16/1/10.
 */
public class HadoopUtil {

    private static final Log logger = LogFactory.getLog(HadoopUtil.class);

    public static final Hadoop getHadoop() {
        return Hadoop.INSTANCE;
    }

    public static enum Hadoop {

        INSTANCE;

        private final Configuration configuration;
        private JobClient jobClient;

        Hadoop() {
            this.configuration = new Configuration();
            try {
                this.jobClient = new JobClient(this.configuration);
            } catch (IOException e) {
                throw new RuntimeException("get io exception when init hadoop object", e);
            }
        }

        public boolean fsMV(String src, String dest) throws IOException {
            Path srcPath = new Path(src);
            Path destPath = new Path(dest);
            return this.fsMV(srcPath, destPath);
        }

        public boolean fsMV(Path src, Path dest) throws IOException {
            logger.info(String.format("moving file %s to dest %s", src, dest));
            FileSystem fileSystem = src.getFileSystem(this.configuration);
            return fileSystem.rename(src, dest);
        }

        public List<FileStatus> fsLs(String pathStr) throws IOException {
            Path path = new Path(pathStr);
            return this.fsLs(path);
        }

        public List<FileStatus> fsLs(Path path) throws IOException {
            FileStatus[] resultList = null;
            logger.info("hadoop fs -ls " + path.toString());
            FileSystem fileSystem = path.getFileSystem(new Configuration());
            if (fileSystem.exists(path)) {
                if (fileSystem.isFile(path)) {
                    return Arrays.asList(fileSystem.getFileStatus(path));
                } else if (fileSystem.isDirectory(path)) {
                    resultList = fileSystem.listStatus(path);
                }
            } else {
                resultList = fileSystem.globStatus(path);
            }
            if (resultList != null) {
                return Arrays.asList(resultList);
            } else {
                return Collections.emptyList();
            }
        }

        public boolean exists(String path) throws IOException {
            return this.exists(new Path(path));
        }

        public boolean exists(Path path) throws IOException {
            FileSystem fileSystem = path.getFileSystem(new Configuration());
            return fileSystem.exists(path);
        }

        public Iterator<String> fsCat(String pathStr) throws IOException {
            Path path = new Path(pathStr);
            return fsCat(path);
        }

        public Iterator<String> fsCat(Path path) throws IOException {
            return new HDFSFileIterator(path);
        }

        public boolean fsMkDir(String pathStr) throws IOException {
            Path path = new Path(pathStr);
            return fsMkDir(path);
        }

        public boolean fsMkDir(Path path) throws IOException {
            logger.info("fsMkDir: " + path.toString());
            FileSystem fileSystem = path.getFileSystem(new Configuration());
            if (!fileSystem.exists(path)) {
                return fileSystem.mkdirs(path);
            } else {
                return true;
            }
        }

        public void fsPut(String localSrc, String dest) throws IOException {
            Path localPath = new Path(localSrc);
            Path destPath = new Path(dest);
            fsPut(localPath, destPath);
        }


        public void fsPut(Path localPath, Path destPath) throws IOException {
            logger.info(String.format("fs put from %s to %s", localPath.toString(), destPath.toString()));
            FileSystem fileSystem = destPath.getFileSystem(this.configuration);
            fileSystem.copyFromLocalFile(false, localPath, destPath);
        }

        public void fsGet(String src, String localDest) throws IOException {
            Path srcPath = new Path(src);
            Path localPath = new Path(localDest);
            fsGet(srcPath, localPath);
        }

        public void fsGet(Path srcPath, Path localPath) throws IOException {
            logger.info(String.format("get %s to local path %s", srcPath.toString(), localPath.toString()));
            srcPath.getFileSystem(new Configuration()).copyToLocalFile(false, srcPath, localPath);
        }

        public void fsRm(String pathStr) throws IOException {
            logger.info("hadoop fs rm path: " + pathStr);
            Path path = new Path(pathStr);
            fsRm(path);
        }

        public void fsRm(Path path) throws IOException {
            FileSystem fileSystem = path.getFileSystem(new Configuration());
            if (!path.toString().contains("*")) {
                fileSystem.delete(path, true);
            } else {
                for (FileStatus fileStatus : this.fsLs(path)) {
                    fileSystem.delete(fileStatus.getPath(), true);
                    logger.info(String.format("file: %s deleted", fileStatus.getPath().toString()));
                }
            }
        }

        public long fsDu(String path) throws IOException {
            return fsDu(new Path(path));
        }

        public long fsDu(Path path) throws IOException {
            long result = 0;
            FileSystem fileSystem = path.getFileSystem(new Configuration());
            if (!fileSystem.exists(path)) {
                result = 0;
            } else if (fileSystem.isDirectory(path)) {
                for (FileStatus fileStatus : this.fsLs(path)) {
                    result += this.fsDu(fileStatus.getPath());
                }
            } else {
                result = fileSystem.getFileStatus(path).getLen();
            }
            return result;
        }

        public boolean fsCp(String src, String dest) throws IOException {
            Path srcPath = new Path(src);
            Path destPath = new Path(dest);
            return fsCp(srcPath, destPath);
        }

        public boolean fsCp(Path srcPath, Path destPath) throws IOException {
            return FileUtil.copy(srcPath.getFileSystem(this.configuration), srcPath,
                    destPath.getFileSystem(this.configuration), destPath,
                    false, true, this.configuration);
        }

        public void chown(String path, String owner, String group) throws IOException {
            this.chown(new Path(path), owner, group);
        }

        public void chown(Path path, String owner, String group) throws IOException {
            FileSystem fileSystem = path.getFileSystem(this.configuration);
            fileSystem.setOwner(path, owner, group);
            if (fileSystem.isDirectory(path)) {
                for (FileStatus fileStatus : fileSystem.listStatus(path)) {
                    chown(fileStatus.getPath(), owner, group);
                }
            }
        }

        public long getLastModificationTime(String path) throws IOException {
            if (isEmpty(path)) {
                return 0l;
            }
            return getLastModificationTime(new Path(path));
        }
        public static boolean isEmpty(String string) {
            if (org.apache.commons.lang.StringUtils.isBlank(string) || "null".equalsIgnoreCase(string)) {
                return true;
            }
            return false;
        }
        public long getLastModificationTime(Path path) throws IOException {
            long lastModificationTime = 0l;
            FileStatus lastFileStatus = null;
            if (exists(path)) {
                FileSystem fileSystem = getFileSystem(path);
                FileStatus fileStatus = fileSystem.getFileStatus(path);
                lastModificationTime = fileStatus.getModificationTime();
                if (fileStatus.isDirectory()) {
                    boolean haveChild = false;
                    for (FileStatus entry : fileSystem.listStatus(path)) {
                        haveChild = true;
                        if (entry.getModificationTime() > lastModificationTime) {
                            lastFileStatus = entry;
                            lastModificationTime = lastFileStatus.getModificationTime();
                        }
                    }
                    if (lastFileStatus != null && haveChild) {
                        return Math.max(lastModificationTime, getLastModificationTime(lastFileStatus.getPath()));
                    }
                }
            }
            return lastModificationTime;
        }

        public FileSystem getFileSystem(Path path) throws IOException {
            return path.getFileSystem(new Configuration());
        }
    }

    public static class HDFSFileIterator implements Iterator<String> {

        private static final Log logger = LogFactory.getLog(HDFSFileIterator.class);

        private final Iterator<FileStatus> fileIterator;
        private final CompressionCodecFactory codecFactory;
        private final FileSystem fileSystem;

        private Path currentPath;
        private BufferedReader currentReader;
        private String nextLine;

        HDFSFileIterator(Path path) throws IOException {
            logger.info("reading file with path " + path.toString());
            this.codecFactory = new CompressionCodecFactory(new Configuration());
            this.fileIterator = HadoopUtil.getHadoop().fsLs(path).iterator();
            this.fileSystem = path.getFileSystem(new Configuration());
        }

        public boolean hasNext() {
            try {
                if (currentReader != null) {
                    this.nextLine = currentReader.readLine();
                    if (nextLine == null) {
                        currentReader.close();
                        currentReader = null;
                        logger.info("close reader for file: " + this.currentPath.toString());
                    } else {
                        return true;
                    }
                }
                if (fileIterator.hasNext()) {
                    nextFile();
                    return hasNext();
                }
            } catch (IOException e) {
                throw new RuntimeException("get IO Exception when read file: " + currentPath.toString(), e);
            }
            return false;
        }

        private void nextFile() throws IOException {
            FileStatus fileStatus = this.fileIterator.next();
            this.currentPath = fileStatus.getPath();
            logger.info("reading file: " + currentPath.toString());
            if (fileStatus.getLen() != 0 || fileStatus.isDirectory()) {
                FSDataInputStream fileIn = this.fileSystem.open(fileStatus.getPath());
                final CompressionCodec codec = this.codecFactory.getCodec(fileStatus.getPath());
                InputStreamReader inputStreamReader;
                if (codec == null) {
                    logger.info("no codec found for file: " + currentPath.toString());
                    inputStreamReader = new InputStreamReader(fileIn);
                } else {
                    inputStreamReader = new InputStreamReader(codec.createInputStream(fileIn));
                }
                this.currentReader = new BufferedReader(inputStreamReader);
            } else {
                logger.info(String.format("skip file %s for zero length or is a directory", this.currentPath));
            }
        }

        public String next() {
            return this.nextLine;
        }

        public void remove() {
            throw new UnsupportedOperationException("do not support method action");
        }
    }
}
