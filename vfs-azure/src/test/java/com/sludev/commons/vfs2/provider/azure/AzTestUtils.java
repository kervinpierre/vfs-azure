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
import java.io.IOException;
import java.nio.file.Path;
import junit.framework.Assert;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;

/**
 *
 * @author kervin
 */
public class AzTestUtils
{
    public static void uploadFile(String accntName, String accntHost, String accntKey, String containerName,
                                       Path localFile, Path remotePath) throws FileSystemException
    {
        DefaultFileSystemManager currMan = new DefaultFileSystemManager();
        currMan.addProvider(AzConstants.AZSBSCHEME, new AzFileProvider());
        currMan.addProvider("file", new DefaultLocalFileProvider());
        currMan.init(); 
        
        StaticUserAuthenticator auth = new StaticUserAuthenticator("", accntName, accntKey);
        FileSystemOptions opts = new FileSystemOptions(); 
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth); 
        
        String currUriStr = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, accntHost, containerName, remotePath);
        FileObject currFile = currMan.resolveFile(currUriStr, opts);
        FileObject currFile2 = currMan.resolveFile(
                String.format("file://%s", localFile));
        
        currFile.copyFrom(currFile2, Selectors.SELECT_SELF);
        
        currFile.close();
        currMan.close();
    }
    
    public static void deleteFile(String accntName, String accntHost, String accntKey, String containerName,
                                       Path remotePath) throws FileSystemException
    {
        DefaultFileSystemManager currMan = new DefaultFileSystemManager();
        currMan.addProvider(AzConstants.AZSBSCHEME, new AzFileProvider());
        currMan.init(); 
        
        StaticUserAuthenticator auth = new StaticUserAuthenticator("", accntName, accntKey);
        FileSystemOptions opts = new FileSystemOptions(); 
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth); 
        
        String currUriStr = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, accntHost, containerName, remotePath);
        FileObject currFile = currMan.resolveFile(currUriStr, opts);
        
        Boolean delRes = currFile.delete();
        Assert.assertTrue(delRes);
    }
    
    public static File createTempFile(String prefix, String ext, String content) throws IOException
    {
        File res = File.createTempFile(prefix, ext);
        try(FileWriter fw = new FileWriter(res))
        {
            BufferedWriter bw = new BufferedWriter(fw);
            bw.append(content);
            bw.flush();
        }
        
        return res;
    }
}