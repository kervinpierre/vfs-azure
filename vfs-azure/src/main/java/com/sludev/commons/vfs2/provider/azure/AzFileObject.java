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

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerProperties;
import com.microsoft.azure.storage.blob.BlobInputStream;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.provider.URLFileName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main FileObject class in this provider.  It holds most of the API callbacks
 * for the provider.
 * 
 * @author Kervin Pierre
 */
public class AzFileObject extends AbstractFileObject
{
    private static final Logger log = LoggerFactory.getLogger(AzFileObject.class);
    
    private final AzFileSystem fileSystem;
    private CloudBlobContainer currContainer;
    private CloudBlockBlob currBlob;
    private BlobContainerProperties currContainerProperties;
    private BlobProperties currBlobProperties;
    
    /**
     * Creates a new FileObject for use with a remote Azure Blob Storage file or folder.
     * 
     * @param name
     * @param fileSystem 
     */
    protected AzFileObject(final AbstractFileName name, final AzFileSystem fileSystem)
    {
        super(name, fileSystem);
        this.fileSystem = fileSystem;
        
        currContainer = null;
        currBlob = null;
        currBlobProperties = null;
        currContainerProperties = null;
    }

    /**
     * Convenience method that returns the container and path from the current URL.
     * 
     * @return A tuple containing the container name and the path.
     */
    protected Pair<String, String> getContainerAndPath()
    {
        Pair<String, String> res = null;
        
        try
        {
            URLFileName currName = (URLFileName)getName();
           
            String currNameStr = currName.getPath();
            currNameStr = StringUtils.stripStart(currNameStr, "/");
            
            if( StringUtils.isBlank(currNameStr) )
            {
                log.warn( 
                        String.format("getContainerAndPath() : Path '%s' does not appear to be valid", currNameStr));
                
                return null;
            }
            
            // Deal with the special case of the container root.
            if( StringUtils.contains(currNameStr, "/") == false )
            {
                // Container and root
                return new ImmutablePair<>(currNameStr, "/");
            }
            
            String[] resArray = StringUtils.split(currNameStr, "/", 2);
            
            res = new ImmutablePair<>(resArray[0], resArray[1]);
        }
        catch (Exception ex)
        {
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
    protected void doAttach() throws Exception
    {
        Pair<String, String> path = getContainerAndPath();
        
        try
        {
            // Check the container.  Force a network call so we can fail-fast
            currContainer 
                = fileSystem.getClient().getContainerReference(path.getLeft()); 
        }
        catch (RuntimeException ex)
        {
            log.error( String.format("doAttach() Exception for '%s' : '%s'", 
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
    protected FileType doGetType() throws Exception
    {
        FileType res;

        Pair<String, String> path = getContainerAndPath();

        if( currBlob.exists() )
        {
            res = FileType.FILE;
        }
        else
        {
            // Blob Service does not have folders.  Just files with path separators in
            // their names.
            
            // Here's the trick for folders.
            //
            // Do a listing on that prefix.  If it returns anything, after not
            // existing, then it's a folder.
            String prefix = path.getRight();
            if( prefix.endsWith("/") == false )
            {
                // We need folders ( prefixes ) to end with a slash
                prefix += "/";
            }

            Iterable<ListBlobItem> blobs = null;
            if( prefix.equals("/") )
            {
                // Special root path case. List the root blobs with no prefix
                blobs = currContainer.listBlobs();
            }
            else
            {
                blobs = currContainer.listBlobs(prefix);
            }
            
            if( blobs.iterator().hasNext() )
            {
                res = FileType.FOLDER;
            }
            else
            {
                res = FileType.IMAGINARY;
            }
        }
        
        return res;
    }

    /**
     * Lists the children of this file.  Is only called if {@link #doGetType}
     * returns {@link FileType#FOLDER}.  The return value of this method
     * is cached, so the implementation can be expensive.<br />
     * @return a possible empty String array if the file is a directory or null or an exception if the
     * file is not a directory or can't be read.
     * @throws Exception if an error occurs.
     */
    @Override
    protected String[] doListChildren() throws Exception
    {
        String[] res = null;
        
        Pair<String, String> path = getContainerAndPath();

        String prefix = path.getRight();
        if( prefix.endsWith("/") == false )
        {
            // We need folders ( prefixes ) to end with a slash
            prefix += "/";
        }
        
        Iterable<ListBlobItem> blobs = null;
        if( prefix.equals("/") )
        {
            // Special root path case. List the root blobs with no prefix
            blobs = currContainer.listBlobs();
        }
        else
        {
            blobs = currContainer.listBlobs(prefix);
        }
        
        List<ListBlobItem> blobList = new ArrayList<>();
        
        // Pull it all in memory and work from there
        CollectionUtils.addAll(blobList, blobs);
        ArrayList<String> resList = new ArrayList<>();
        for(ListBlobItem currBlob : blobList )
        {
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

    private void checkBlobProperties() throws StorageException
    {
        if( currBlobProperties == null )
        {
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
    protected long doGetContentSize() throws Exception
    {
        long res = -1;
        
        checkBlobProperties();
        res = currBlobProperties.getLength();
        
        return res;
    }

    /**
     * Get an InputStream for reading the content of this File Object.
     * @return The InputStream object for reading.
     * @throws Exception 
     */
    @Override
    protected InputStream doGetInputStream() throws Exception
    {
        BlobInputStream in = currBlob.openInputStream();
        
        return in;
    }

    /**
     * Callback for handling delete on this File Object
     * @throws Exception 
     */
    @Override
    protected void doDelete() throws Exception
    {
        // Use deleteIfExists() to simplify recursive deletes.
        // Otherwise VFS will call delete() on an empty folder, and we know
        // folders do not really exist.  Especially empty ones.
        currBlob.deleteIfExists();
    }

    /**
     * Callback for handling create folder requests.  Since there are no folders
     * in Azure Cloud Storage this call is ingored.
     * 
     * @throws Exception 
     */
    @Override
    protected void doCreateFolder() throws Exception
    {
        log.info(String.format("doCreateFolder() called."));
    }

    /**
     * Used for creating folders.  It's not used since Azure Cloud Storage does not have
     * the concept of folders.
     * 
     * @throws FileSystemException 
     */
    @Override
    public void createFolder() throws FileSystemException
    {
        log.debug(String.format("createFolder() called."));
    }

    /**
     * Callback for getting an OutputStream for writing into Azure Blob Storage file.
     * @param bAppend  bAppend true if the file should be appended to, false if it should be overwritten.
     * @return
     * @throws Exception 
     */
    @Override
    protected OutputStream doGetOutputStream(boolean bAppend) throws Exception
    {
        OutputStream res;

        res = currBlob.openOutputStream();
        
        return res;
    }

    /**
     * Callback for use when detaching this File Object from Azure Blob Storage.
     * 
     * The File Object should be reusable after <code>attach()</code> call.
     * @throws Exception 
     */
    @Override
    protected void doDetach() throws Exception
    {
        currBlob = null;
        currContainer = null;
        currBlobProperties = null;
        currContainerProperties = null;
    }

    /**
     * Callback for handling the <code>getLastModifiedTime()</code> Commons VFS API call.
     * 
     * @return Time since the file has last been modified
     * @throws Exception 
     */
    @Override
    protected long doGetLastModifiedTime() throws Exception
    {
        long res;
        
        checkBlobProperties();
        Date lm = currBlobProperties.getLastModified();
        
        res = lm.getTime();
        
        return res;
    }
}
