/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sludev.commons.vfs2.provider.azure;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobInputStream;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.net.io.CopyStreamListener;
import org.apache.commons.vfs2.FileNotFolderException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.NameScope;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.provider.URLFileName;
import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 * The main FileObject class in this provider.  It holds most of the API callbacks
 * for the provider.
 *
 * @author Kervin Pierre
 */
public class AzFileObject extends AbstractFileObject {

    private static final Logger log = LoggerFactory.getLogger(AzFileObject.class);

    private final AzFileSystem fileSystem;
    private CloudBlobContainer currContainer;
    private CloudBlockBlob currBlob;
    private BlobProperties currBlobProperties;

    private static final int MEGABYTES_TO_BYTES_MULTIPLIER = (int) Math.pow(2.0, 20.0);

    private static Boolean ENABLE_AZURE_STORAGE_LOG = false;

    static Integer UPLOAD_BLOCK_SIZE = 3; //in MB's

    private static Tika tika = new Tika();

    static {

        String uploadBlockSizeProperty = System.getProperty("azure.upload.block.size");
        UPLOAD_BLOCK_SIZE = (int) NumberUtils.toLong(uploadBlockSizeProperty, UPLOAD_BLOCK_SIZE) * MEGABYTES_TO_BYTES_MULTIPLIER;

        String enableAzureLogging = System.getProperty("azure.enable.logging");
        if (StringUtils.isNotEmpty(enableAzureLogging)) {
            ENABLE_AZURE_STORAGE_LOG = BooleanUtils.toBoolean(enableAzureLogging);
        }

        log.info("Azure upload block size : {} Bytes, concurrent request count: {}", UPLOAD_BLOCK_SIZE);
    }

    public CloudBlockBlob getCurrBlob() {

        return this.currBlob;
    }


    /**
     * Creates a new FileObject for use with a remote Azure Blob Storage file or folder.
     *
     * @param name
     * @param fileSystem
     */
    protected AzFileObject(final AbstractFileName name, final AzFileSystem fileSystem) {

        super(name, fileSystem);
        this.fileSystem = fileSystem;

        currContainer = null;
        currBlob = null;
        currBlobProperties = null;
    }


    /**
     * Convenience method that returns the container and path from the current URL.
     *
     * @return A tuple containing the container name and the path.
     */
    protected Pair<String, String> getContainerAndPath() {

        Pair<String, String> res = null;

        try {
            URLFileName currName = (URLFileName) getName();

            String currNameStr = currName.getPath();
            currNameStr = StringUtils.stripStart(currNameStr, "/");

            if (StringUtils.isBlank(currNameStr)) {
                log.warn(
                        String.format("getContainerAndPath() : Path '%s' does not appear to be valid", currNameStr));

                return null;
            }

            // Deal with the special case of the container root.
            if (StringUtils.contains(currNameStr, "/") == false) {
                // Container and root
                return new ImmutablePair<>(currNameStr, "/");
            }

            String[] resArray = StringUtils.split(currNameStr, "/", 2);

            res = new ImmutablePair<>(resArray[0], resArray[1]);
        }
        catch (Exception ex) {
            log.error(
                    String.format("getContainerAndPath() : Path does not appear to be valid"), ex);
        }

        return res;
    }


    /**
     * Callback used when this FileObject is first used.  We connect to the remote
     * server and check early so we can 'fail-fast'.  If there are no issues then
     * this FileObject can be used.
     *
     * @throws Exception
     */
    @Override
    protected void doAttach() throws Exception {

        Pair<String, String> path = getContainerAndPath();

        try {
            // Check the container.  Force a network call so we can fail-fast
            currContainer
                    = fileSystem.getClient().getContainerReference(path.getLeft());
        }
        catch (RuntimeException ex) {
            log.error(String.format("doAttach() Exception for '%s' : '%s'",
                    path.getLeft(), path.getRight()), ex);

            throw ex;
        }

        currBlob = currContainer.getBlockBlobReference(path.getRight());
    }


    /**
     * Callback for checking the type of the current FileObject.  Typically can
     * be of type...
     * FILE for regular remote files
     * FOLDER for regular remote containers
     * IMAGINARY for a path that does not exist remotely.
     *
     * @return
     * @throws Exception
     */
    @Override
    protected FileType doGetType() throws Exception {

        FileType res;

        URLFileName currName = (URLFileName) getName();

        if (currName != null && currName.getType() == FileType.FOLDER) {
            return FileType.FOLDER;
        }

        if (currBlob.exists()) {
            res = FileType.FILE;
        }
        else {
            // Blob Service does not have folders.  Just files with path separators in their names.

            // Here's the trick for folders.
            //
            // Do a listing on that prefix.  If it returns anything, after not existing, then it's a folder.
            Pair<String, String> path = getContainerAndPath();
            String prefix = path.getRight();
            if (prefix.endsWith("/") == false) {
                // We need folders ( prefixes ) to end with a slash
                prefix += "/";
            }

            Iterable<ListBlobItem> blobs = null;
            if (prefix.equals("/")) {
                // Special root path case. List the root blobs with no prefix
                blobs = currContainer.listBlobs();
            }
            else {
                blobs = currContainer.listBlobs(prefix);
            }

            if (blobs.iterator().hasNext()) {
                res = FileType.FOLDER;
            }
            else {
                res = FileType.IMAGINARY;
            }
        }

        return res;
    }


    /**
     * Lists the children of this file.  Is only called if {@link #doGetType}
     * returns {@link FileType#FOLDER}.  The return value of this method
     * is cached, so the implementation can be expensive.<br />
     *
     * @return a possible empty String array if the file is a directory or null or an exception if the
     * file is not a directory or can't be read.
     * @throws Exception if an error occurs.
     */
    @Override
    protected String[] doListChildren() throws Exception {

        String[] res = null;

        Pair<String, String> path = getContainerAndPath();

        String prefix = path.getRight();
        if (prefix.endsWith("/") == false) {
            // We need folders ( prefixes ) to end with a slash
            prefix += "/";
        }

        Iterable<ListBlobItem> blobs = null;
        if (prefix.equals("/")) {
            // Special root path case. List the root blobs with no prefix
            blobs = currContainer.listBlobs();
        }
        else {
            blobs = currContainer.listBlobs(prefix);
        }

        List<ListBlobItem> blobList = new ArrayList<>();

        // Pull it all in memory and work from there
        CollectionUtils.addAll(blobList, blobs);
        ArrayList<String> resList = new ArrayList<>();
        for (ListBlobItem currBlob : blobList) {
            String currBlobStr = currBlob.getUri().getPath();
            resList.add(currBlobStr);
        }

        res = resList.toArray(new String[resList.size()]);

        return res;
    }

    //    @Override
    //    protected FileObject[] doListChildrenResolved() throws Exception
    //    {
    //        FileObject[] res = null;
    //
    //        Pair<String, String> path = getContainerAndPath();
    //
    //        String prefix = path.getRight();
    //        if( prefix.endsWith("/") == false )
    //        {
    //            // We need folders ( prefixes ) to end with a slash
    //            prefix += "/";
    //        }
    //
    //        Iterable<ListBlobItem> blobs = null;
    //        if( prefix.equals("/") )
    //        {
    //            // Special root path case. List the root blobs with no prefix
    //            blobs = currContainer.listBlobs();
    //        }
    //        else
    //        {
    //            blobs = currContainer.listBlobs(prefix);
    //        }
    //
    //        List<ListBlobItem> blobList = new ArrayList<>();
    //
    //        // Pull it all in memory and work from there
    //        CollectionUtils.addAll(blobList, blobs);
    //        ArrayList<AzFileObject> resList = new ArrayList<>();
    //        for(ListBlobItem currBlobItem : blobList )
    //        {
    //            String currBlobStr = currBlobItem.getUri().getPath();
    //            AzFileObject childBlob = new AzFileObject();
    //            FileName currName = getFileSystem().getFileSystemManager().resolveName(name, file, NameScope.CHILD);
    //
    //            resList.add(currBlobStr);
    //        }
    //
    //        res = resList.toArray(new String[resList.size()]);
    //
    //        return res;
    //    }


    private void checkBlobProperties() throws StorageException {

        if (currBlobProperties == null) {
            currBlob.downloadAttributes();
            currBlobProperties = currBlob.getProperties();
        }
    }


    /**
     * Callback for handling "content size" requests by the provider.
     *
     * @return The number of bytes in the File Object's content
     * @throws Exception
     */
    @Override
    protected long doGetContentSize() throws Exception {

        long res = -1;

        checkBlobProperties();
        res = currBlobProperties.getLength();

        return res;
    }


    /**
     * Get an InputStream for reading the content of this File Object.
     *
     * @return The InputStream object for reading.
     * @throws Exception
     */
    @Override
    protected InputStream doGetInputStream() throws Exception {

        BlobInputStream in = currBlob.openInputStream();

        return in;
    }


    /**
     * Callback for handling delete on this File Object
     *
     * @throws Exception
     */
    @Override
    protected void doDelete() throws Exception {
        // Use deleteIfExists() to simplify recursive deletes.
        // Otherwise VFS will call delete() on an empty folder, and we know
        // folders do not really exist.  Especially empty ones.
        currBlob.deleteIfExists();
    }


    /**
     * Callback for handling create folder requests.  Since there are no folders
     * in Azure Cloud Storage this call is ignored.
     *
     * @throws Exception
     */
    @Override
    protected void doCreateFolder() throws Exception {

        log.info(String.format("doCreateFolder() called."));
    }


    /**
     * Used for creating folders.  It's not used since Azure Cloud Storage does not have
     * the concept of folders.
     *
     * @throws FileSystemException
     */
    @Override
    public void createFolder() throws FileSystemException {

        log.debug(String.format("createFolder() called."));
    }


    /**
     * Callback for getting an OutputStream for writing into Azure Blob Storage file.
     *
     * @param bAppend bAppend true if the file should be appended to, false if it should be overwritten.
     * @return
     * @throws Exception
     */
    @Override
    protected OutputStream doGetOutputStream(boolean bAppend) throws Exception {

        OutputStream res = currBlob.openOutputStream();

        return res;
    }


    /**
     * Callback for use when detaching this File Object from Azure Blob Storage.
     * <p>
     * The File Object should be reusable after <code>attach()</code> call.
     *
     * @throws Exception
     */
    @Override
    protected void doDetach() throws Exception {

        currBlob = null;
        currContainer = null;
        currBlobProperties = null;
    }


    /**
     * Callback for handling the <code>getLastModifiedTime()</code> Commons VFS API call.
     *
     * @return Time since the file has last been modified
     * @throws Exception
     */
    @Override
    protected long doGetLastModifiedTime() throws Exception {

        long res;

        checkBlobProperties();
        Date lm = currBlobProperties.getLastModified();

        res = lm.getTime();

        return res;
    }


    /**
     * We need to override this method, because the parent one throws an exception.
     *
     * @param modtime the last modified time to set.
     * @return true if setting the last modified time was successful.
     * @throws Exception
     */
    @Override
    protected boolean doSetLastModifiedTime(long modtime) throws Exception {

        return true;
    }


    /**
     * Returns the list of children.
     *
     * @return The list of children
     * @throws FileSystemException If there was a problem listing children
     * @see AbstractFileObject#getChildren()
     */
    @Override
    public FileObject[] getChildren() throws FileSystemException {

        try {
            // Folders which are copied from other folders, have type = IMAGINARY. We can not throw exception based on folder
            // type only and so we have check here for content.
            if (getType().hasContent()) {
                throw new FileNotFolderException(getName());
            }
        }
        catch (Exception ex) {
            throw new FileNotFolderException(getName(), ex);
        }

        return super.getChildren();
    }


    /**
     * Override to use Azure Blob Java Client library in upload. This is efficient then using default.
     */
    @Override
    public void copyFrom(final FileObject file, final FileSelector selector)
            throws FileSystemException {

        this.copyFrom(file, selector, null);
    }


    public void copyFrom(FileObject file, FileSelector selector, CopyStreamListener copyStreamListener)
            throws FileSystemException {

        log.debug("Inside AZFileObject copy");

        if (!file.exists()) {
            throw new FileSystemException("vfs.provider/copy-missing-file.error", file);
        }
        else {
            ArrayList files = new ArrayList();
            file.findFiles(selector, false, files);
            int count = files.size();

            for (int i = 0; i < count; ++i) {
                FileObject srcFile = (FileObject) files.get(i);
                String relPath = file.getName().getRelativeName(srcFile.getName());
                FileObject destFile = this.resolveFile(relPath, NameScope.DESCENDENT_OR_SELF);
                if (destFile.exists() && destFile.getType() != srcFile.getType()) {
                    destFile.delete(Selectors.SELECT_ALL);
                }

                // We need to requires the CloudBlockBlob for the that we want to upload, as we were always using the
                // CloudBlockBlob of the root directory when we were trying to copy directories, hence it was always overwriting
                // the root directory on azure storage.
                CloudBlockBlob fileCurrBlob = getFileCurrBlob(destFile);

                try {
                    if (srcFile.getType().hasChildren()) {
                        destFile.createFolder();
                    }
                    else if (canCopyServerSide(srcFile, destFile)) {
                        CloudBlockBlob currDestinationBlob = ((AzFileObject) destFile).getCurrBlob();
                        CloudBlockBlob currSourceBlob = ((AzFileObject) srcFile).getCurrBlob();
                        try {
                            currDestinationBlob.startCopy(currSourceBlob);
                        }
                        catch (URISyntaxException e) {
                            throw new FileSystemException("vfs.provider/copy-file.error", new Object[] { srcFile, destFile }, e);
                        }
                        finally {
                            destFile.close();
                            srcFile.close();
                        }
                    }
                    else if (srcFile.getType().hasContent()) {
                        try {

                            InputStream sourceStream = srcFile.getContent().getInputStream();
                            long length = srcFile.getContent().getSize();
                            if (UPLOAD_BLOCK_SIZE != null) {
                                fileCurrBlob.setStreamWriteSizeInBytes(UPLOAD_BLOCK_SIZE);
                            }

                            BlobProperties fileCurrBlobProperties = fileCurrBlob.getProperties();

                            if (fileCurrBlobProperties != null) {
                                String fileName = srcFile.getName().getBaseName();
                                String contentType = tika.detect(fileName);

                                log.debug("Content type is {} for {} file", contentType, fileName);

                                if (contentType != null) {
                                    fileCurrBlobProperties.setContentType(contentType);
                                }
                            }
                            else {
                                log.debug("currBlobProperties is null");
                            }

                            OperationContext opContext = new OperationContext();
                            opContext.setLoggingEnabled(ENABLE_AZURE_STORAGE_LOG);

                            fileCurrBlob.upload(sourceStream, length, null, null, opContext);

                        }
                        finally {
                            destFile.close();
                            srcFile.close();
                        }
                    }
                    else {
                        // nothing useful to do if no content and can't have children
                        throw new FileSystemException("vfs.provider/copy-file.error", new Object[] { srcFile, destFile },
                                new UnsupportedOperationException());
                    }
                }
                catch (IOException io) {
                    throw new FileSystemException("vfs.provider/copy-file.error", new Object[] { srcFile, destFile }, io);
                }
                catch (StorageException se) {
                    throw new FileSystemException("vfs.provider/copy-file.error", new Object[] { srcFile, destFile }, se);
                }
            }

        }

        log.debug("Exit AZFileObject copy");
    }


    /**
     * Returns the file CloudBlockBlob of the give file.
     * If the file is not the type of AzFileObject or does not have its own CloudBlockBlob it is going to return the
     * CloudBlockBlob of the current file.
     */
    private CloudBlockBlob getFileCurrBlob(FileObject destFile) {

        CloudBlockBlob cloudBlockBlob = currBlob;
        if (AzFileObject.class.isAssignableFrom(destFile.getClass())) {

            cloudBlockBlob = ((AzFileObject) destFile).getCurrBlob();
            if (cloudBlockBlob == null) {

                cloudBlockBlob = currBlob;
            }

        }

        return cloudBlockBlob;
    }


    /**
     * Compares account and container name to check possibilities of copying file using server to server copy option
     *
     * @param sourceFileObject
     * @param destinationFileObject
     * @return
     */
    private boolean canCopyServerSide(FileObject sourceFileObject, FileObject destinationFileObject) {

        if (!(sourceFileObject instanceof AzFileObject) || !(destinationFileObject instanceof AzFileObject)) {
            return false;
        }

        // No point to copy server side if their file system is different
        if (!(sourceFileObject.getFileSystem() == destinationFileObject.getFileSystem())) {
            return false;
        }

        AzFileObject azSourceFileObject = (AzFileObject) sourceFileObject;
        AzFileObject azDestinationFileObject = (AzFileObject) destinationFileObject;

        String sourceAccountName = getAccountName(azSourceFileObject);

        String destinationAccountName = getAccountName(azDestinationFileObject);

        if (sourceAccountName != null
                && destinationAccountName != null
                && sourceAccountName.equalsIgnoreCase(destinationAccountName)) {

            String srcContainerName = azSourceFileObject.getContainerAndPath().getKey();
            String destContainerName = azDestinationFileObject.getContainerAndPath().getKey();

            return srcContainerName.equalsIgnoreCase(destContainerName);
        }

        return false;
    }


    /**
     * Returns an account name from given azure file object
     *
     * @param azFileObject
     * @return
     */
    private String getAccountName(AzFileObject azFileObject) {

        AzFileSystem azFileSystem = (AzFileSystem) azFileObject.getFileSystem();

        return azFileSystem.getClient() != null ? azFileSystem.getClient().getCredentials().getAccountName() : null;
    }


    /**
     * Returns false to reply on copyFrom method in case moving/copying file within same azure container
     *
     * @param fileObject
     * @return
     */
    @Override
    public boolean canRenameTo(FileObject fileObject) {

        return false;
    }
}
