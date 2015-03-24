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
package com.sludev.commons.vfs.simpleshell;

import com.sludev.commons.vfs2.provider.azure.AzConstants;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author kervin
 */
public class SimpleShellTest
{
    private static final Logger log 
            = LoggerFactory.getLogger(SimpleShellTest.class);
    
    private Properties testProperties;
    
    public SimpleShellTest()
    {
    }
    
    @Rule
    public TestWatcher testWatcher = new SimpleShellTestWatcher();
    
    @Before
    public void setUp() 
    {
        
        /**
         * Get the current test properties from a file so we don't hard-code
         * in our source code.
         */
        testProperties = SimpleShellProperties.GetProperties();
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
     * Test of rm method, of class SimpleShell.
     */
    @Test
    public void testRm001() throws Exception
    {
        String currAccountStr = testProperties.getProperty("azure.account.name"); // .blob.core.windows.net
        String currKey = testProperties.getProperty("azure.account.key");
        String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
        String currFileNameStr = "file05";
        
        String currUriStr = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, currAccountStr, currContainerStr, currFileNameStr);
        
        String[] cmd = new String[2];
        SimpleShell instance = new SimpleShell(currAccountStr, currKey, currContainerStr);
        
        cmd[0] = "rm";
        cmd[1] = currUriStr;
        
        instance.rm(cmd);
    }

    /**
     * Test of cp method, of class SimpleShell.
     */
    @Test
    public void testCp001() throws Exception
    {
        String currAccountStr = testProperties.getProperty("azure.account.name"); // .blob.core.windows.net
        String currKey = testProperties.getProperty("azure.account.key");
        String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
        
        String currFileNameStr = "file05";
        String currSrcUriStr = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, currAccountStr, currContainerStr, currFileNameStr);
        
        currFileNameStr = "testFld/file06";
        String currDestUriStr = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, currAccountStr, currContainerStr, currFileNameStr);
        
        String[] cmd = new String[3];
        SimpleShell instance = new SimpleShell(currAccountStr, currKey, currContainerStr);
        
        cmd[0] = "cp";
        cmd[1] = currSrcUriStr;
        cmd[2] = currDestUriStr;
        
        instance.cp(cmd);
        
        /**
         * FIXME : Do exists() on destination
         */
    }

    /**
     * Test of cat method, of class SimpleShell.
     */
    @Test
    public void testCat001() throws Exception
    {
        String currAccountStr = testProperties.getProperty("azure.account.name"); // .blob.core.windows.net
        String currKey = testProperties.getProperty("azure.account.key");
        String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
        String currFileNameStr = "file05";
        
        String currUriStr = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, currAccountStr, currContainerStr, currFileNameStr);
        
        String[] cmd = new String[2];
        SimpleShell instance = new SimpleShell(currAccountStr, currKey, currContainerStr);
        
        cmd[0] = "cat";
        cmd[1] = currUriStr;
        
        instance.cat(cmd);
        
        /**
         * FIXME : We should test instance for failure
         */
    }

    /**
     * Test of ls method, of class SimpleShell.
     * 
     */
    @Test
    public void testLs001() throws Exception
    {
        String currAccountStr = testProperties.getProperty("azure.account.name"); // .blob.core.windows.net
        String currKey = testProperties.getProperty("azure.account.key");
        String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
        String currFileNameStr = "";
        
        String currUriStr = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, currAccountStr, currContainerStr, currFileNameStr);
        
        String[] cmd = new String[2];
        SimpleShell instance = new SimpleShell(currAccountStr, currKey, currContainerStr);
        
        cmd[0] = "ls";
        cmd[1] = currUriStr;
        
        instance.ls(cmd);
        
        /**
         * FIXME : We should test instance for failure
         */
    }
    
    /**
     * Test of touch method, of class SimpleShell.
     */
    @Test
    public void testTouch() throws Exception
    {
        String currAccountStr = testProperties.getProperty("azure.account.name"); // .blob.core.windows.net
        String currKey = testProperties.getProperty("azure.account.key");
        String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
        String currFileNameStr = "file05";
        
        String currUriStr = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, currAccountStr, currContainerStr, currFileNameStr);
        
        String[] cmd = new String[2];
        SimpleShell instance = new SimpleShell(currAccountStr, currKey, currContainerStr);
        
        cmd[0] = "touch";
        cmd[1] = currUriStr;
        
        instance.touch(cmd);
    }
    
}
