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

import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import java.util.Collection;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File-System object represents a connect to Microsoft Azure Blob via a single client.
 * 
 * @author Kervin Pierre
 */
public class AzFileSystem
    extends AbstractFileSystem
    implements FileSystem
{
    private static final Logger log = LoggerFactory.getLogger(AzFileSystem.class);
    
    private final CloudBlobClient client;

    static BlobRequestOptions defaultRequestOptions = new BlobRequestOptions();

    static {
        int MEGABYTES_TO_BYTES_MULTIPLIER = (int) Math.pow(2.0, 20.0);

        Integer singleBlockSize = null;

        String singleBlockSizeProperty = System.getProperty("azure.single.upload.block.size");
        if (NumberUtils.isNumber(singleBlockSizeProperty)) {
            singleBlockSize = (int) NumberUtils.toLong(singleBlockSizeProperty) * MEGABYTES_TO_BYTES_MULTIPLIER;
        }

        log.info("Azure single upload block size : " + singleBlockSize + " Bytes");

        Integer parallelUploadThread = 2;

        String threadProperty = System.getProperty("azure.parallel.upload.thread");
        if (NumberUtils.isNumber(threadProperty)) {
            parallelUploadThread = NumberUtils.toInt(threadProperty);
        }

        log.info("Azure parallel upload threads : " + parallelUploadThread);

        defaultRequestOptions.setSingleBlobPutThresholdInBytes(singleBlockSize);
        defaultRequestOptions.setConcurrentRequestCount(parallelUploadThread);
    }


    /**
     * The single client for interacting with Azure Blob Storage.
     * 
     * @return 
     */
    protected CloudBlobClient getClient()
    {
        return client;
    }
    
    protected AzFileSystem(final GenericFileName rootName, final CloudBlobClient client,
                             final FileSystemOptions fileSystemOptions)
    {
        super(rootName, null, fileSystemOptions);
        this.client = client;
        this.client.setDefaultRequestOptions(defaultRequestOptions);
    }
    
    @Override
    protected FileObject createFile(AbstractFileName name) throws Exception
    {
        return new AzFileObject(name, this);
    }

    @Override
    protected void addCapabilities(Collection<Capability> caps)
    {
        caps.addAll(AzFileProvider.capabilities);
    }
    
}
