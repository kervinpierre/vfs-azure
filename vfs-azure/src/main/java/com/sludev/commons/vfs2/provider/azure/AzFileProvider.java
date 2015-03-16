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

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticationData;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.apache.commons.vfs2.util.UserAuthenticatorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author kervin
 */
public class AzFileProvider
                  extends AbstractOriginatingFileProvider
{
    private static final Logger log = LoggerFactory.getLogger(AzFileProvider.class);
    
    public static final UserAuthenticationData.Type[] AUTHENTICATOR_TYPES = new UserAuthenticationData.Type[]
        {
            UserAuthenticationData.USERNAME, UserAuthenticationData.PASSWORD
        };
    
    static final Collection<Capability> capabilities = Collections.unmodifiableCollection(Arrays.asList(new Capability[]
    {
        Capability.GET_TYPE,
        Capability.READ_CONTENT,
        Capability.APPEND_CONTENT,
        Capability.URI,
        Capability.GET_LAST_MODIFIED,
        Capability.ATTRIBUTES,
        Capability.RANDOM_ACCESS_READ,
        Capability.DIRECTORY_READ_CONTENT,
    }));

    public AzFileProvider()
    {
        super();
        setFileNameParser(AzFileNameParser.getInstance());
    }
    
    @Override
    protected FileSystem doCreateFileSystem(FileName rootName, FileSystemOptions fileSystemOptions) throws FileSystemException
    {
        AzFileSystem fileSystem = null;
        GenericFileName genRootName = (GenericFileName)rootName;
        
        StorageCredentials storageCreds;
        CloudStorageAccount storageAccount;
        CloudBlobClient client;
        
        UserAuthenticationData authData = null;
        try
        {
            authData = UserAuthenticatorUtils.authenticate(fileSystemOptions, AUTHENTICATOR_TYPES);
            
            String currAcct = UserAuthenticatorUtils.toString(UserAuthenticatorUtils.getData(authData,
                    UserAuthenticationData.USERNAME, UserAuthenticatorUtils.toChar(genRootName.getUserName())));
            
            String currKey =  UserAuthenticatorUtils.toString(UserAuthenticatorUtils.getData(authData,
                    UserAuthenticationData.PASSWORD, UserAuthenticatorUtils.toChar(genRootName.getPassword())));
        
            storageCreds = new StorageCredentialsAccountAndKey(currAcct, currKey);           
            storageAccount = new CloudStorageAccount(storageCreds);
            
            client = storageAccount.createCloudBlobClient();
            
            fileSystem = new AzFileSystem(genRootName, client, fileSystemOptions);
        }
        catch (URISyntaxException ex)
        {
            throw new FileSystemException(ex);
        }
        finally
        {
            UserAuthenticatorUtils.cleanup(authData);
        }
        
        return fileSystem;
    }

    @Override
    public Collection<Capability> getCapabilities()
    {
        return capabilities;
    }
    
}
