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

import com.microsoft.azure.storage.blob.BlobContainerProperties;
import com.microsoft.azure.storage.blob.BlobInputStream;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URI;
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
 *
 * @author kervin
 */
public class AzFileObject extends AbstractFileObject
{
    private static final Logger log = LoggerFactory.getLogger(AzFileObject.class);
    
    private final AzFileSystem fileSystem;
    
    protected AzFileObject(final AbstractFileName name, final AzFileSystem fileSystem)
    {
        super(name, fileSystem);
        this.fileSystem = fileSystem;
        
    }

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
    
    @Override
    protected void doAttach() throws Exception
    {
        Pair<String, String> path = getContainerAndPath();
        CloudBlobContainer currContainer = null;
        
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
        
        BlobContainerProperties currContainerProperties = currContainer.getProperties();
        String containerEtag = currContainerProperties.getEtag();
        
        URI containerUri = currContainer.getUri();
        
        CloudBlockBlob currBlob = currContainer.getBlockBlobReference(path.getRight());
        boolean existRes = currBlob.exists();
    }
    
    @Override
    protected FileType doGetType() throws Exception
    {
        FileType res;
        
        // Let's not cache, intead contact the data store every time.
        
        Pair<String, String> path = getContainerAndPath();
        
        // Check the container.  Force a network call.
        CloudBlobContainer currContainer 
                = fileSystem.getClient().getContainerReference(path.getLeft());
        CloudBlockBlob currBlob = currContainer.getBlockBlobReference(path.getRight());
        
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
        
        CloudBlobContainer currContainer 
                = fileSystem.getClient().getContainerReference(path.getLeft());
        
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

    /**
     * 
     * This call is tried first before doListChildren() allowing the FileObject to resolve objects
     * directly.
     * 
     * Currently I prefer AbstractFileObject resolving the files.  So I'll let it do so.
     * 
     */
    
//    @Override
//    protected FileObject[] doListChildrenResolved() throws Exception
//    {
//        FileObject[] res;
//        List<AzFileObject> resList = new ArrayList<>();
//        
//        String[] childArray = doListChildren();
//        for( String currUrl : childArray )
//        {
//            AzFileObject currFO = (AzFileObject)this.resolveFile(currUrl);
//            resList.add(currFO);
//        }
//        
//        res = resList.toArray(new FileObject[resList.size()]);
//        
//        return res;
//    }

    @Override
    protected long doGetContentSize() throws Exception
    {
        long res = -1;
        
        Pair<String, String> path = getContainerAndPath();
        
        CloudBlobContainer currContainer 
                = fileSystem.getClient().getContainerReference(path.getLeft());
        CloudBlockBlob currBlob = currContainer.getBlockBlobReference(path.getRight());
        currBlob.downloadAttributes();
        BlobProperties props = currBlob.getProperties();
        
        res = props.getLength();
        
        return res;
    }

    @Override
    protected InputStream doGetInputStream() throws Exception
    {
        Pair<String, String> path = getContainerAndPath();
        
        CloudBlobContainer currContainer 
                = fileSystem.getClient().getContainerReference(path.getLeft());
        CloudBlockBlob currBlob = currContainer.getBlockBlobReference(path.getRight());
        
        BlobInputStream in = currBlob.openInputStream();
        
        return in;
    }

    @Override
    protected void doDelete() throws Exception
    {
        Pair<String, String> path = getContainerAndPath();
        
        CloudBlobContainer currContainer 
                = fileSystem.getClient().getContainerReference(path.getLeft());
        CloudBlockBlob currBlob = currContainer.getBlockBlobReference(path.getRight());
        
        // Purposely use the more restrictive delete() over deleteIfExists()
        currBlob.delete();
    }

    @Override
    protected void doCreateFolder() throws Exception
    {
        log.info(String.format("doCreateFolder() called."));
    }

    @Override
    public void createFolder() throws FileSystemException
    {
        log.info(String.format("createFolder() called."));
    }

    /**
     * 
     * @param bAppend  bAppend true if the file should be appended to, false if it should be overwritten.
     * @return
     * @throws Exception 
     */
    @Override
    protected OutputStream doGetOutputStream(boolean bAppend) throws Exception
    {
        OutputStream res;
        
        Pair<String, String> path = getContainerAndPath();
        
        CloudBlobContainer currContainer 
                = fileSystem.getClient().getContainerReference(path.getLeft());
        CloudBlockBlob currBlob = currContainer.getBlockBlobReference(path.getRight());
        
        res = currBlob.openOutputStream();
        
        return res;
    }

    @Override
    protected long doGetLastModifiedTime() throws Exception
    {
        long res;
        
        Pair<String, String> path = getContainerAndPath();
        
        CloudBlobContainer currContainer 
                = fileSystem.getClient().getContainerReference(path.getLeft());
        CloudBlockBlob currBlob = currContainer.getBlockBlobReference(path.getRight());
        
        currBlob.downloadAttributes();
        BlobProperties props = currBlob.getProperties();
        
        Date lm = props.getLastModified();
        
        res = lm.getTime();
        
        return res;
    }

    @Override
    protected boolean doSetLastModifiedTime(long modtime) throws Exception
    {
        boolean res;
        
        Pair<String, String> path = getContainerAndPath();
        
        CloudBlobContainer currContainer 
                = fileSystem.getClient().getContainerReference(path.getLeft());
        CloudBlockBlob currBlob = currContainer.getBlockBlobReference(path.getRight());
        
        currBlob.downloadAttributes();
        
        BlobProperties props = currBlob.getProperties();
        Date currDate = props.getLastModified();
        
        Method setLastModified = props.getClass().getDeclaredMethod("setLastModified", Date.class);
        
        Date lm = new Date(modtime);
        setLastModified.setAccessible(true);
        setLastModified.invoke(props, lm);
        currBlob.uploadProperties();
        
        res = true;
        
        return res;
    }
    
    
}
