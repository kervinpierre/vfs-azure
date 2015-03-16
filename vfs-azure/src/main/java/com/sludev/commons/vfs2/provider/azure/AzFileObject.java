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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
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
            
            String[] resArray = StringUtils.split(currNameStr, "/", 2);
            
            res = new ImmutablePair<>(resArray[0], resArray[1]);
        }
        catch (Exception ex)
        {
            // TODO : Log exception.  Every file should have a container
        }
        
        return res;
    }
    
    @Override
    protected void doAttach() throws Exception
    {
        super.doAttach(); 
        
        Pair<String, String> path = getContainerAndPath();
        
        // Check the container.  Force a network call.
        CloudBlobContainer currContainer 
                = fileSystem.getClient().getContainerReference(path.getLeft());
        
        BlobContainerProperties currContainerProperties = currContainer.getProperties();
        String containerEtag = currContainerProperties.getEtag();
        
        URI containerUri = currContainer.getUri();
        
        CloudBlockBlob currBlob = currContainer.getBlockBlobReference(path.getRight());
        URI blogUri = currBlob.getUri();
        
        BlobProperties currBlobProperties = currBlob.getProperties();
        String blobEtag = currBlobProperties.getEtag(); 
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
            res = FileType.IMAGINARY;
        }
        
        return res;
    }

    @Override
    protected String[] doListChildren() throws Exception
    {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected long doGetContentSize() throws Exception
    {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
    
}
