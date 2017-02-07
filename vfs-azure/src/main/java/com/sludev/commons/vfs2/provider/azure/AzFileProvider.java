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
import org.apache.commons.vfs2.UserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.apache.commons.vfs2.util.UserAuthenticatorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main provider class in the Azure Blob Commons VFS provider.
 * 
 * This class can be declared and passed to the current File-system manager 
 * 
 * E.g....
 * <pre><code>
 * // Grab some credentials. The "testProperties" class is just a regular properties class
 * // Retrieve the properties however you please
 * String currAccountStr = testProperties.getProperty("azure.account.name"); // .blob.core.windows.net
 * String currKey = testProperties.getProperty("azure.account.key");
 * String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
 * String currHost = testProperties.getProperty("azure.host");  // &lt;account&gt;.blob.core.windows.net
 * String currFileNameStr;
 * 
 * // Now let's create a temp file just for upload
 * File temp = File.createTempFile("uploadFile01", ".tmp");
 * try(FileWriter fw = new FileWriter(temp))
 * {
 *     BufferedWriter bw = new BufferedWriter(fw);
 *     bw.append("testing...");
 *     bw.flush();
 * }
 * 
 * // Create an Apache Commons VFS manager option and add 2 providers. Local file and Azure.
 * // All done programmatically
 * DefaultFileSystemManager currMan = new DefaultFileSystemManager();
 * currMan.addProvider(AzConstants.AZBSSCHEME, new AzFileProvider());
 * currMan.addProvider("file", new DefaultLocalFileProvider());
 * currMan.init(); 

 * // Create a new Authenticator for the credentials
 * StaticUserAuthenticator auth = new StaticUserAuthenticator("", currAccountStr, currKey);
 * FileSystemOptions opts = new FileSystemOptions(); 
 * DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth); 
 * 
 * // Create a URL for creating this remote file
 * currFileNameStr = "test01.tmp";
 * String currUriStr = String.format("%s://%s/%s/%s", 
 *                    AzConstants.AZBSSCHEME, currHost, currContainerStr, currFileNameStr);
 * 
 * // Resolve the imaginary file remotely.  So we have a file object
 * FileObject currFile = currMan.resolveFile(currUriStr, opts);
 * 
 * // Resolve the local file for upload
 * FileObject currFile2 = currMan.resolveFile(
 *         String.format("file://%s", temp.getAbsolutePath()));
 * 
 * // Use the API to copy from one local file to the remote file 
 * currFile.copyFrom(currFile2, Selectors.SELECT_SELF);
 * 
 * // Delete the temp we don't need anymore
 * temp.delete();
 * </code></pre>
 * 
 * @author Kervin Pierre
 */
public class AzFileProvider
                  extends AbstractOriginatingFileProvider
{
    private static final Logger log = LoggerFactory.getLogger(AzFileProvider.class);
    
    private static final FileSystemOptions DEFAULT_OPTIONS = new FileSystemOptions();
    
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
        Capability.ATTRIBUTES,
        Capability.RANDOM_ACCESS_READ,
        Capability.DIRECTORY_READ_CONTENT,
        Capability.LIST_CHILDREN,
        Capability.LAST_MODIFIED,
        Capability.GET_LAST_MODIFIED,
        Capability.CREATE,
        Capability.DELETE
    }));

    private String endpoint;
    
    /**
     * Set the S3 endpoint we should use.  This needs to be done before init() is called if done at all.
     * 
     * The value is optional, hence best to leave undeclared.
     * 
     * @param ep The optional endpoint E.g. <account>..blob.core.windows.net 
     */
    public void setEndpoint(String ep)
    {
        endpoint = ep;
    }
    
    /**
     * Construct a new provider for use with a File-System Manager object.
     */
    public AzFileProvider()
    {
        super();
        setFileNameParser(AzFileNameParser.getInstance());
    }
    
    /**
     * In the case that we are not sent FileSystemOptions object, we need to have
     * one handy.
     * 
     * @return 
     */
    public static FileSystemOptions getDefaultFileSystemOptions()
    {
        return DEFAULT_OPTIONS;
    }
    
    /**
     * Callback for handling the create File-System event
     * 
     * @param rootName
     * @param fileSystemOptions
     * @return
     * @throws FileSystemException 
     */
    @Override
    protected FileSystem doCreateFileSystem(FileName rootName, FileSystemOptions fileSystemOptions) throws FileSystemException
    {
        AzFileSystem fileSystem = null;
        GenericFileName genRootName = (GenericFileName)rootName;
        
        StorageCredentials storageCreds;
        CloudStorageAccount storageAccount;
        CloudBlobClient client;
        
        FileSystemOptions currFSO = (fileSystemOptions != null) ? fileSystemOptions : getDefaultFileSystemOptions();
        UserAuthenticator ua = DefaultFileSystemConfigBuilder.getInstance().getUserAuthenticator(currFSO);

        UserAuthenticationData authData = null;
        try
        {
            authData = ua.requestAuthentication(AUTHENTICATOR_TYPES);
            
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

    /**
     * Get this provider's list of capabilities.
     * 
     * @return 
     */
    @Override
    public Collection<Capability> getCapabilities()
    {
        return capabilities;
    }
    
}
