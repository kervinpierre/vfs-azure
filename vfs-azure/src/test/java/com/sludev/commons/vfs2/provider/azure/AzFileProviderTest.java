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
import java.util.Properties;
import junit.framework.Assert;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author kervin
 */
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
    public void tearDown()
    {
    }

    /**
     * 
     */
    @Test
    public void uploadFile01() throws Exception
    {
        String currAccountStr = testProperties.getProperty("azure.account.name"); // .blob.core.windows.net
        String currKey = testProperties.getProperty("azure.account.key");
        String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
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
                           AzConstants.AZSBSCHEME, currAccountStr, currContainerStr, currFileNameStr);
        FileObject currFile = currMan.resolveFile(currUriStr, opts);
        FileObject currFile2 = currMan.resolveFile(
                String.format("file://%s", temp.getAbsolutePath()));
        
        currFile.copyFrom(currFile2, Selectors.SELECT_SELF);
    }
    
    @Test
    public void downloadFile01() throws Exception
    {
        String currAccountStr = testProperties.getProperty("azure.account.name"); // .blob.core.windows.net
        String currKey = testProperties.getProperty("azure.account.key");
        String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
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
                           AzConstants.AZSBSCHEME, currAccountStr, currContainerStr, currFileNameStr);
        FileObject currFile = currMan.resolveFile(currUriStr, opts);
        
        String destStr = String.format("file://%s", temp.getAbsolutePath());
        FileObject currFile2 = currMan.resolveFile( destStr );
        
        log.info( String.format("copying '%s' to '%s'", currUriStr, destStr));
        
        currFile2.copyFrom(currFile, Selectors.SELECT_SELF);
    }
    
    @Test
    public void deleteFile01() throws Exception
    {
        String currAccountStr = testProperties.getProperty("azure.account.name"); // .blob.core.windows.net
        String currKey = testProperties.getProperty("azure.account.key");
        String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
        String currFileNameStr;
        
        DefaultFileSystemManager currMan = new DefaultFileSystemManager();
        currMan.addProvider(AzConstants.AZSBSCHEME, new AzFileProvider());
        currMan.init(); 
        
        StaticUserAuthenticator auth = new StaticUserAuthenticator("", currAccountStr, currKey);
        FileSystemOptions opts = new FileSystemOptions(); 
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth); 
        
        currFileNameStr = "test01.tmp";
        String currUriStr = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, currAccountStr, currContainerStr, currFileNameStr);
        FileObject currFile = currMan.resolveFile(currUriStr, opts);
        
        log.info( String.format("deleting '%s'", currUriStr));
        
        Boolean delRes = currFile.delete();
        Assert.assertTrue(delRes);
    }
    
    @Test
    public void exist01() throws Exception
    {
        String currAccountStr = testProperties.getProperty("azure.account.name"); // .blob.core.windows.net
        String currKey = testProperties.getProperty("azure.account.key");
        String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
        String currFileNameStr;
        
        DefaultFileSystemManager currMan = new DefaultFileSystemManager();
        currMan.addProvider(AzConstants.AZSBSCHEME, new AzFileProvider());
        currMan.init(); 
        
        StaticUserAuthenticator auth = new StaticUserAuthenticator("", currAccountStr, currKey);
        FileSystemOptions opts = new FileSystemOptions(); 
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth); 
        
        currFileNameStr = "test01.tmp";
        String currUriStr = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, currAccountStr, currContainerStr, currFileNameStr);
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
}
