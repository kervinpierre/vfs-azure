/*
 *   SLU Dev Inc. CONFIDENTIAL
 *   DO NOT COPY
 *  
 *  Copyright (c) [2012] - [2015] SLU Dev Inc. <info@sludev.com>
 *  All Rights Reserved.
 *  
 *  NOTICE:  All information contained herein is, and remains
 *   the property of SLU Dev Inc. and its suppliers,
 *   if any.  The intellectual and technical concepts contained
 *   herein are proprietary to SLU Dev Inc. and its suppliers and
 *   may be covered by U.S. and Foreign Patents, patents in process,
 *   and are protected by trade secret or copyright law.
 *   Dissemination of this information or reproduction of this material
 *   is strictly forbidden unless prior written permission is obtained
 *   from SLU Dev Inc.
 */
package com.sludev.commons.vfs2.provider.azure;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
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
    public static void uploadFile(String accntName, String accntKey, String containerName,
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
                           AzConstants.AZSBSCHEME, accntName, containerName, remotePath);
        FileObject currFile = currMan.resolveFile(currUriStr, opts);
        FileObject currFile2 = currMan.resolveFile(
                String.format("file://%s", localFile));
        
        currFile.copyFrom(currFile2, Selectors.SELECT_SELF);
        
        currFile.close();
        currMan.close();
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