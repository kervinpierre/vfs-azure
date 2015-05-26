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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Paths;
import java.util.Properties;
import junit.framework.Assert;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author kervin
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AzFileProviderTest
{
    private static final Logger log = LoggerFactory.getLogger(AzFileProviderTest.class);
    
    private Properties testProperties;
    
    public AzFileProviderTest()
    {
    }
    
    @Rule
    public TestWatcher testWatcher = new AzTestWatcher();
    
    @Before
    public void setUp()
    {
        
        /**
         * Get the current test properties from a file so we don't hard-code
         * in our source code.
         */
        testProperties = AzTestProperties.GetProperties();
        
        try
        {
            /**
             * Setup the remote folders for testing
             */
            uploadFileSetup02();
        }
        catch (Exception ex)
        {
            log.debug("Error setting up remote folder structure.  Have you set the test001.properties file?", ex);
        }
    }
    
    @BeforeClass
    public static void setUpClass()
    {
    }
    
    @AfterClass
    public static void tearDownClass()
    {
    }
    
    @After
    public void tearDown() throws Exception
    {
        removeFileSetup02();
    }

    /**
     * 
     */
    @Test
    public void A001_uploadFile() throws Exception
    {
        String currAccountStr = testProperties.getProperty("azure.account.name");
        String currKey = testProperties.getProperty("azure.account.key");
        String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
        String currHost = testProperties.getProperty("azure.host");  // <account>.blob.core.windows.net
        String currFileNameStr;
        
        File temp = File.createTempFile("uploadFile01", ".tmp");
        try(FileWriter fw = new FileWriter(temp))
        {
            BufferedWriter bw = new BufferedWriter(fw);
            bw.append("testing...");
            bw.flush();
        }
        
        DefaultFileSystemManager currMan = new DefaultFileSystemManager();
        currMan.addProvider(AzConstants.AZSBSCHEME, new AzFileProvider());
        currMan.addProvider("file", new DefaultLocalFileProvider());
        currMan.init(); 
        
        StaticUserAuthenticator auth = new StaticUserAuthenticator("", currAccountStr, currKey);
        FileSystemOptions opts = new FileSystemOptions(); 
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth); 
        
        currFileNameStr = "test01.tmp";
        String currUriStr = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, currHost, currContainerStr, currFileNameStr);
        FileObject currFile = currMan.resolveFile(currUriStr, opts);
        FileObject currFile2 = currMan.resolveFile(
                String.format("file://%s", temp.getAbsolutePath()));
        
        currFile.copyFrom(currFile2, Selectors.SELECT_SELF);
        temp.delete();
    }
    
    @Test
    public void A002_downloadFile() throws Exception
    {
        String currAccountStr = testProperties.getProperty("azure.account.name"); 
        String currKey = testProperties.getProperty("azure.account.key");
        String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
        String currHost = testProperties.getProperty("azure.host"); // <account>.blob.core.windows.net
        String currFileNameStr;
        
        File temp = File.createTempFile("downloadFile01", ".tmp");
        
        DefaultFileSystemManager currMan = new DefaultFileSystemManager();
        currMan.addProvider(AzConstants.AZSBSCHEME, new AzFileProvider());
        currMan.addProvider("file", new DefaultLocalFileProvider());
        currMan.init(); 
        
        StaticUserAuthenticator auth = new StaticUserAuthenticator("", currAccountStr, currKey);
        FileSystemOptions opts = new FileSystemOptions(); 
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth); 
        
        currFileNameStr = "test01.tmp";
        String currUriStr = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, currHost, currContainerStr, currFileNameStr);
        FileObject currFile = currMan.resolveFile(currUriStr, opts);
        
        String destStr = String.format("file://%s", temp.getAbsolutePath());
        FileObject currFile2 = currMan.resolveFile( destStr );
        
        log.info( String.format("copying '%s' to '%s'", currUriStr, destStr));
        
        currFile2.copyFrom(currFile, Selectors.SELECT_SELF);
    }
    
    @Test
    public void A003_exist() throws Exception
    {
        String currAccountStr = testProperties.getProperty("azure.account.name"); 
        String currKey = testProperties.getProperty("azure.account.key");
        String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
        String currHost = testProperties.getProperty("azure.host");  // <account>.blob.core.windows.net
        String currFileNameStr;
        
        DefaultFileSystemManager currMan = new DefaultFileSystemManager();
        currMan.addProvider(AzConstants.AZSBSCHEME, new AzFileProvider());
        currMan.init(); 
        
        StaticUserAuthenticator auth = new StaticUserAuthenticator("", currAccountStr, currKey);
        FileSystemOptions opts = new FileSystemOptions(); 
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth); 
        
        currFileNameStr = "test01.tmp";
        String currUriStr = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, currHost, currContainerStr, currFileNameStr);
        FileObject currFile = currMan.resolveFile(currUriStr, opts);
        
        log.info( String.format("exist() file '%s'", currUriStr));
        
        Boolean existRes = currFile.exists();
        Assert.assertTrue(existRes);
        
        
        currFileNameStr = "non-existant-file-8632857264.tmp";
        currUriStr = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, currAccountStr, currContainerStr, currFileNameStr);
        currFile = currMan.resolveFile(currUriStr, opts);
        
        log.info( String.format("exist() file '%s'", currUriStr));
        
        existRes = currFile.exists();
        Assert.assertFalse(existRes);
    }
    
    @Test
    public void A004_getContentSize() throws Exception
    {
        String currAccountStr = testProperties.getProperty("azure.account.name"); 
        String currKey = testProperties.getProperty("azure.account.key");
        String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
        String currHost = testProperties.getProperty("azure.host");  // <account>.blob.core.windows.net
        String currFileNameStr;
        
        DefaultFileSystemManager currMan = new DefaultFileSystemManager();
        currMan.addProvider(AzConstants.AZSBSCHEME, new AzFileProvider());
        currMan.init(); 
        
        StaticUserAuthenticator auth = new StaticUserAuthenticator("", currAccountStr, currKey);
        FileSystemOptions opts = new FileSystemOptions(); 
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth); 
        
        currFileNameStr = "test01.tmp";
        String currUriStr = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, currHost, currContainerStr, currFileNameStr);
        FileObject currFile = currMan.resolveFile(currUriStr, opts);
        
        log.info( String.format("exist() file '%s'", currUriStr));
        
        FileContent cont = currFile.getContent();
        long contSize = cont.getSize();
        
        Assert.assertTrue(contSize>0);
        
    }
  
    /**
     * By default FileObject.getChildren() will use doListChildrenResolved() if available
     * 
     * @throws Exception 
     */
    @Test
    public void A005_listChildren() throws Exception
    {
        String currAccountStr = testProperties.getProperty("azure.account.name"); 
        String currKey = testProperties.getProperty("azure.account.key");
        String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
        String currHost = testProperties.getProperty("azure.host");  // <account>.blob.core.windows.net
        
        DefaultFileSystemManager currMan = new DefaultFileSystemManager();
        currMan.addProvider(AzConstants.AZSBSCHEME, new AzFileProvider());
        currMan.init(); 
        
        StaticUserAuthenticator auth = new StaticUserAuthenticator("", currAccountStr, currKey);
        FileSystemOptions opts = new FileSystemOptions(); 
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth); 
        
        String currFileNameStr = "uploadFile02";
        String currUriStr = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, currHost, currContainerStr, currFileNameStr);
        FileObject currFile = currMan.resolveFile(currUriStr, opts);
        
        FileObject[] currObjs = currFile.getChildren();
        for(FileObject obj : currObjs)
        {
            FileName currName = obj.getName();
            Boolean res = obj.exists();
            FileType ft = obj.getType();
            
            log.info( String.format("\nNAME.PATH : '%s'\nEXISTS : %b\nTYPE : %s\n\n", 
                           currName.getPath(), res, ft));
        }
    }
    
    @Test
    public void A006_testContent() throws Exception
    {
        String currAccountStr = testProperties.getProperty("azure.account.name"); 
        String currKey = testProperties.getProperty("azure.account.key");
        String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
        String currHost = testProperties.getProperty("azure.host");  // <account>.blob.core.windows.net
        String currFileNameStr;
        
        DefaultFileSystemManager currMan = new DefaultFileSystemManager();
        currMan.addProvider(AzConstants.AZSBSCHEME, new AzFileProvider());
        currMan.init(); 
        
        StaticUserAuthenticator auth = new StaticUserAuthenticator("", currAccountStr, currKey);
        FileSystemOptions opts = new FileSystemOptions(); 
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth); 
        
        currFileNameStr = "file05";
        String currUriStr = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, currHost, currContainerStr, currFileNameStr);
        FileObject currFile = currMan.resolveFile(currUriStr, opts);
        
        FileContent content = currFile.getContent();
        long size = content.getSize();
        Assert.assertTrue( size >= 0);
        
        long modTime = content.getLastModifiedTime();
        Assert.assertTrue(modTime>0);
    }
    
    @Test
    public void A007_deleteFile() throws Exception
    {
        String currAccountStr = testProperties.getProperty("azure.account.name"); 
        String currKey = testProperties.getProperty("azure.account.key");
        String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
        String currHost = testProperties.getProperty("azure.host");  // <account>.blob.core.windows.net
        String currFileNameStr;
        
        DefaultFileSystemManager currMan = new DefaultFileSystemManager();
        currMan.addProvider(AzConstants.AZSBSCHEME, new AzFileProvider());
        currMan.init(); 
        
        StaticUserAuthenticator auth = new StaticUserAuthenticator("", currAccountStr, currKey);
        FileSystemOptions opts = new FileSystemOptions(); 
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth); 
        
        currFileNameStr = "test01.tmp";
        String currUriStr = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, currHost, currContainerStr, currFileNameStr);
        FileObject currFile = currMan.resolveFile(currUriStr, opts);
        
        log.info( String.format("deleting '%s'", currUriStr));
        
        Boolean delRes = currFile.delete();
        Assert.assertTrue(delRes);
    }
    
    public void removeFileSetup02() throws Exception
    {
        String currAccountStr = testProperties.getProperty("azure.account.name"); 
        String currKey = testProperties.getProperty("azure.account.key");
        String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
        String currHost = testProperties.getProperty("azure.host");  // <account>.blob.core.windows.net
            
        AzTestUtils.deleteFile(currAccountStr, currHost, currKey, currContainerStr,
                               Paths.get("uploadFile02/dir01/file01"));
              
        AzTestUtils.deleteFile(currAccountStr, currHost, currKey, currContainerStr,
                               Paths.get("uploadFile02/dir01/file02"));
        
        AzTestUtils.deleteFile(currAccountStr, currHost, currKey, currContainerStr,
                               Paths.get("uploadFile02/dir02/file03"));
    
        AzTestUtils.deleteFile(currAccountStr, currHost, currKey, currContainerStr,
                               Paths.get("uploadFile02/file04"));
     
        AzTestUtils.deleteFile(currAccountStr, currHost, currKey, currContainerStr,
                               Paths.get("file05"));
   
        AzTestUtils.deleteFile(currAccountStr, currHost, currKey, currContainerStr,
                               Paths.get("uploadFile02/dir02/file06"));
    }
    
    public void uploadFileSetup02() throws Exception
    {
        String currAccountStr = testProperties.getProperty("azure.account.name"); 
        String currKey = testProperties.getProperty("azure.account.key");
        String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
        String currHost = testProperties.getProperty("azure.host");  // <account>.blob.core.windows.net
        
        File temp = AzTestUtils.createTempFile("uploadFile02", "tmp", "File 01");      
        AzTestUtils.uploadFile(currAccountStr, currHost, currKey, currContainerStr, temp.toPath(),
                               Paths.get("uploadFile02/dir01/file01"));
        temp.delete();
        
        temp = AzTestUtils.createTempFile("uploadFile02", "tmp", "File 02");      
        AzTestUtils.uploadFile(currAccountStr, currHost, currKey, currContainerStr, temp.toPath(),
                               Paths.get("uploadFile02/dir01/file02"));
        temp.delete();
        
        temp = AzTestUtils.createTempFile("uploadFile02", "tmp", "File 03");      
        AzTestUtils.uploadFile(currAccountStr, currHost, currKey, currContainerStr, temp.toPath(),
                               Paths.get("uploadFile02/dir02/file03"));
        temp.delete();
        
        temp = AzTestUtils.createTempFile("uploadFile02", "tmp", "File 04");      
        AzTestUtils.uploadFile(currAccountStr, currHost, currKey, currContainerStr, temp.toPath(),
                               Paths.get("uploadFile02/file04"));
        temp.delete();
        
        temp = AzTestUtils.createTempFile("uploadFile02", "tmp", "File 05");      
        AzTestUtils.uploadFile(currAccountStr, currHost, currKey, currContainerStr, temp.toPath(),
                               Paths.get("file05"));
        temp.delete();
        
        temp = AzTestUtils.createTempFile("uploadFile02", "tmp", "File 06");      
        AzTestUtils.uploadFile(currAccountStr, currHost, currKey, currContainerStr, temp.toPath(),
                               Paths.get("uploadFile02/dir02/file06"));
        temp.delete();
    }
}
