/*
 * Licensed to David Pilato (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package fr.pilato.elasticsearch.crawler.fs.fileabstractor;

import fr.pilato.elasticsearch.crawler.fs.meta.settings.FsSettings;
import fr.pilato.elasticsearch.crawler.fs.meta.settings.Server;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileFilter;
import org.apache.commons.net.ftp.FTPFileFilters;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

public class FileAbstractorFTP extends FileAbstractor<FTPFile> {

    private FTPClient ftp;

    public FileAbstractorFTP(FsSettings fsSettings) {
        super(fsSettings);
    }

    @Override
    public FileAbstractModel toFileAbstractModel(String path, FTPFile file) {
        FileAbstractModel model = new FileAbstractModel();
        model.name = file.getName();
        model.directory = file.isDirectory();
        model.file = !model.directory;
        model.lastModifiedDate = Instant.ofEpochMilli(file.getTimestamp().getTimeInMillis());
        model.path = path;
        model.fullpath = model.path.concat("/").concat(model.name);
        model.size = file.getSize();
        model.owner = file.getUser();
        model.group = file.getGroup();
        return model;
    }

    @Override
    public InputStream getInputStream(FileAbstractModel file) throws Exception {
        return ftp.retrieveFileStream(file.fullpath);
    }

    @Override
    public Collection<FileAbstractModel> getFiles(String dir) throws Exception {
        logger.debug("Listing FTP files in {}", dir);

        ftp.changeWorkingDirectory(dir);
        FTPFile[] files = ftp.listFiles(dir, file -> file != null && !file.getName().equals(".") && !file.getName().equals(".."));

        if (files == null) return null;

        Collection<FileAbstractModel> result = new ArrayList<>(files.length);
        // Iterate other files
        // We ignore here all files like . and ..
        for (FTPFile file : files) {
            result.add(toFileAbstractModel(dir, file));
        }

        logger.debug("{} local files found", result.size());
        return result;
    }

    @Override
    public boolean exists(String dir) throws Exception {
        FTPFile[] files = ftp.listFiles(dir);
        logger.debug("found {} files in dir {}", files != null ? files.length : 0, dir);
        logger.trace("files: {}", (Object) files);
        return files != null && files.length > 0;
    }

    @Override
    public void open() throws Exception {
        ftp = openFTPConnection(fsSettings.getServer());
    }

    @Override
    public void close() throws Exception {
        ftp.disconnect();
    }

    public FTPClient openFTPConnection(Server server) throws Exception {
        logger.debug("Opening FTP connection to {}@{}:{}", server.getUsername(),
                server.getHostname(), server.getPort());
        FTPClient ftp = new FTPClient();

        try {
            ftp.connect(server.getHostname(), server.getPort());
            ftp.login(server.getUsername(), server.getPassword());
            ftp.enterLocalPassiveMode();
            ftp.setFileType(FTP.BINARY_FILE_TYPE);
        } catch (IOException e) {
            logger.warn("Cannot connect with FTP to {}@{}", server.getUsername(),
                    server.getHostname());
            throw new RuntimeException("Can not connect to " + server.getUsername() + "@" + server.getHostname(), e);
        }

        //checking SSH client connection.
        if (!ftp.isConnected()) {
            logger.warn("Cannot connect with FTP to {}@{}", server.getUsername(),
                    server.getHostname());
            throw new RuntimeException("Can not connect to " + server.getUsername() + "@" + server.getHostname());
        }
        logger.debug("FTP connection successful");
        return ftp;
    }
}
