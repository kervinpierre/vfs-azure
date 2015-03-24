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
import com.sludev.commons.vfs2.provider.azure.AzFileProvider;
import com.sludev.commons.vfs2.provider.azure.AzFileSystemConfigBuilder;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import jline.console.ConsoleReader;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.Completer;
import jline.console.completer.FileNameCompleter;
import jline.console.completer.StringsCompleter;
import jline.console.history.FileHistory;

import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.FileUtil;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple command-line shell for performing file operations.
 *
 * @author Kervin Pierre
 * @author <a href="mailto:adammurdoch@apache.org">Adam Murdoch</a>
 * @author Gary D. Gregory
 */
public class SimpleShell
{
    private static final Logger log = LoggerFactory.getLogger(SimpleShell.class);
    
    private FileSystemManager mgr;
    private FileObject cwd;
    
    private Properties mainProperties;
    private ConsoleReader conReader;
    
    public static void main(final String[] args)
    {
        try
        {
            (new SimpleShell()).go();
        }
        catch (Exception ex)
        {
            log.error(String.format("Error running shell"), ex);
            System.exit(1);
        }
        System.exit(0);
    }

    public SimpleShell() throws FileSystemException, IOException
    {
        mainProperties = SimpleShellProperties.GetProperties();
                
        String currAccountStr = mainProperties.getProperty("azure.account.name"); // .blob.core.windows.net
        String currKey = mainProperties.getProperty("azure.account.key");
        String currContainerStr = mainProperties.getProperty("azure.test0001.container.name");
        
        Init(currAccountStr, currKey, currContainerStr);
    }
    
    public SimpleShell(String accnt, String key, String cont) throws FileSystemException, IOException
    {
        Init(accnt, key, cont);
    }
    
    private void Init(String accnt, String key, String cont) throws IOException
    {
        conReader = new ConsoleReader();
        conReader.setPrompt("AzureShell> ");
        
         List<Completer> completors = new LinkedList<>();
         
        String currFileNameStr = "dir1";

        AzFileProvider azfp = new AzFileProvider();
        StaticUserAuthenticator auth = new StaticUserAuthenticator("", accnt, key);
        AzFileSystemConfigBuilder.getInstance().setUserAuthenticator(azfp.getDefaultFileSystemOptions(), auth); 
        
        DefaultFileSystemManager currMan = new DefaultFileSystemManager();
        currMan.addProvider(AzConstants.AZSBSCHEME, azfp);
        currMan.addProvider("file", new DefaultLocalFileProvider());
        currMan.init(); 
        
        mgr = currMan;
        //cwd = mgr.resolveFile(System.getProperty("user.dir"));c
        String currAzURL = String.format("%s://%s/%s/%s", 
                           AzConstants.AZSBSCHEME, accnt, cont, currFileNameStr);
        cwd = mgr.resolveFile(currAzURL);
        
        completors.add(new FileNameCompleter());
        completors.add(new StringsCompleter(AzConstants.AZSBSCHEME, "file://", currAzURL));
        AggregateCompleter aggComp = new AggregateCompleter(completors);
        ArgumentCompleter argComp = new ArgumentCompleter(aggComp);
        argComp.setStrict(false);
        conReader.addCompleter(argComp);
        
        Path histPath = Paths.get(System.getProperty("user.home"), ".simpleshellhist");
        File histFile = histPath.toFile();
        FileHistory fh = new FileHistory(histFile);
        conReader.setHistory(fh);
        conReader.setHistoryEnabled(true);
        
        Runtime.getRuntime().addShutdownHook(
                new Thread() 
                {
                    @Override
                    public void run() 
                    {
                        try
                        {
                            ((FileHistory)conReader.getHistory()).flush();
                        }
                        catch (IOException ex)
                        {
                            log.error("Error saving history", ex);
                        }
                    }
                });
        
    }

    public void go() throws Exception
    {
        String line;
        PrintWriter out = new PrintWriter(conReader.getOutput());

        while ((line = conReader.readLine()) != null) 
        {
            out.println("======>\"" + line + "\"");
            out.flush();

            if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) 
            {
                break;
            }
            
            if (line.equalsIgnoreCase("clear")) 
            {
                conReader.clearScreen();
                continue;
            }
            
            try
            {
                StringTokenizer tokens = new StringTokenizer(line);
                ArrayList<String> cmdList = new ArrayList<>();
                while (tokens.hasMoreTokens())
                {
                    cmdList.add(tokens.nextToken());
                }
                
                String[] cmd = cmdList.toArray(new String[cmdList.size()]);
                
                String cmdName = cmd[0].trim().toLowerCase();
                
                switch(cmdName)
                {
                    case "cat":
                        cat(cmd);
                        break;
                        
                    case "cd":
                        cd(cmd);
                        break;
                        
                    case "cp":
                        cp(cmd);
                        break;
                        
                    case "help":
                        help();
                        break;
                        
                    case "pwd":
                        pwd();
                        break;
                        
                    case "ls":
                        ls(cmd);
                        break;
                        
                    case "rm":
                        rm(cmd);
                        break;
                        
                    case "touch":
                        touch(cmd);
                        break;
                        
                    default:
                        log.warn("Unknown command \"" + cmdName + "\".");
                }

            }
            catch (Exception ex)
            {
                log.error("Command failed:", ex);
            }
        }
    }

    /**
     * Does a 'help' command.
     */
    public void help()
    {
        System.out.println("Commands:");
        System.out.println("cat <file>         Displays the contents of a file.");
        System.out.println("cd [folder]        Changes current folder.");
        System.out.println("cp <src> <dest>    Copies a file or folder.");
        System.out.println("help               Shows this message.");
        System.out.println("ls [-R] [path]     Lists contents of a file or folder.");
        System.out.println("pwd                Displays current folder.");
        System.out.println("rm <path>          Deletes a file or folder.");
        System.out.println("touch <path>       Sets the last-modified time of a file.");
        System.out.println("exit       Exits this program.");
        System.out.println("quit       Exits this program.");
    }

    /**
     * Does an 'rm' command.
     * @param cmd
     * @throws java.lang.Exception
     */
    public void rm(final String[] cmd) throws Exception
    {
        if (cmd.length < 2)
        {
            throw new Exception("USAGE: rm <path>");
        }

        final FileObject file = mgr.resolveFile(cwd, cmd[1]);
        file.delete(Selectors.SELECT_SELF);
    }

    /**
     * Does a 'cp' command.
     * 
     * @param cmd
     * @throws SimpleShellException
     */
    public void cp(String[] cmd) throws SimpleShellException
    {
        if (cmd.length < 3)
        {
            throw new SimpleShellException("USAGE: cp <src> <dest>");
        }

        FileObject src = null;
        try
        {
            src = mgr.resolveFile(cwd, cmd[1]);
        }
        catch (FileSystemException ex)
        {
            String errMsg = String.format("Error resolving source file '%s'", cmd[1]);
            //log.error( errMsg, ex);
            throw new SimpleShellException(errMsg, ex);
        }
        
        FileObject dest = null;
        try
        {
            dest = mgr.resolveFile(cwd, cmd[2]);
        }
        catch (FileSystemException ex)
        {
            String errMsg = String.format("Error resolving destination file '%s'", cmd[2]);
            //log.error( errMsg, ex);
            throw new SimpleShellException(errMsg, ex);
        }
        
        try
        {
            if (dest.exists() && dest.getType() == FileType.FOLDER)
            {
                dest = dest.resolveFile(src.getName().getBaseName());
            }
        }
        catch (FileSystemException ex)
        {
            String errMsg = String.format("Error resolving folder '%s'", cmd[2]);
            //log.error( errMsg, ex);
            throw new SimpleShellException(errMsg, ex);
        }

        try
        {
            dest.copyFrom(src, Selectors.SELECT_ALL);
        }
        catch (FileSystemException ex)
        {
            String errMsg = String.format("Error copyFrom() file '%s' to '%s'", cmd[1], cmd[2]);
            //log.error( errMsg, ex);
            throw new SimpleShellException(errMsg, ex);
        }
    }

    /**
     * Does a 'cat' command.
     * 
     * @param cmd
     * @throws SimpleShellException
     */
    public void cat(String[] cmd) throws SimpleShellException
    {
        if (cmd.length < 2)
        {
            throw new SimpleShellException("USAGE: cat <path>");
        }

        // Locate the file
        FileObject file;
        try
        {
            file = mgr.resolveFile(cwd, cmd[1]);
        }
        catch (FileSystemException ex)
        {
            String errMsg = String.format("Error resolving file '%s'", cmd[1]);
            //log.error( errMsg, ex);
            throw new SimpleShellException(errMsg, ex);
        }

        try
        {
            if( file.exists() == false )
            {
                // Can't cat a file that doesn't exist
                System.out.println( String.format("File '%s' does not exist", cmd[1]) );
                return;
            }
        }
        catch (FileSystemException ex)
        {
            String errMsg = String.format("Error exists() on file '%s'", cmd[1]);
            //log.error( errMsg, ex);
            throw new SimpleShellException(errMsg, ex);
        }
        
        try
        {
            if( file.getType() != FileType.FILE )
            {
                // Only cat files
                System.out.println( String.format("File '%s' is not an actual file.", cmd[1]) );
                return;
            }
        }
        catch (FileSystemException ex)
        {
            String errMsg = String.format("Error getType() on file '%s'", cmd[1]);
            //log.error( errMsg, ex);
            throw new SimpleShellException(errMsg, ex);
        }
        
        try
        {
            // Dump the contents to System.out
            FileUtil.writeContent(file, System.out);
        }
        catch (IOException ex)
        {
            String errMsg = String.format("Error WriteContent() on file '%s'", cmd[1]);
            //log.error( errMsg, ex);
            throw new SimpleShellException(errMsg, ex);
        }
        
        System.out.println();
    }

    /**
     * Does a 'pwd' command.
     */
    public void pwd()
    {
        System.out.println("Current folder is " + cwd.getName());
    }

    /**
     * Does a 'cd' command.
     * If the taget directory does not exist, a message is printed to <code>System.err</code>.
     */
    public void cd(final String[] cmd) throws Exception
    {
        final String path;
        if (cmd.length > 1)
        {
            path = cmd[1];
        }
        else
        {
            path = System.getProperty("user.home");
        }

        // Locate and validate the folder
        FileObject tmp = mgr.resolveFile(cwd, path);
        if (tmp.exists())
        {
            cwd = tmp;
        }
        else
        {
            System.out.println("Folder does not exist: " + tmp.getName());
        }
        System.out.println("Current folder is " + cwd.getName());
    }

    /**
     * Does an 'ls' command.
     * 
     * @param cmd
     * @throws org.apache.commons.vfs2.FileSystemException
     */
    public void ls(final String[] cmd) throws FileSystemException
    {
        int pos = 1;
        final boolean recursive;
        if (cmd.length > pos && cmd[pos].equals("-R"))
        {
            recursive = true;
            pos++;
        }
        else
        {
            recursive = false;
        }

        final FileObject file;
        if (cmd.length > pos)
        {
            file = mgr.resolveFile(cwd, cmd[pos]);
        }
        else
        {
            file = cwd;
        }

        switch(file.getType())
        {
            case FOLDER:
                // List the contents
                System.out.println("Contents of " + file.getName());
                listChildren(file, recursive, "");
                break;
        
            case FILE:
                // Stat the file
                System.out.println(file.getName());
                final FileContent content = file.getContent();
                System.out.println("Size: " + content.getSize() + " bytes.");
                final DateFormat dateFormat = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.MEDIUM);
                final String lastMod = dateFormat.format(new Date(content.getLastModifiedTime()));
                System.out.println("Last modified: " + lastMod);
                break;
                
            case IMAGINARY:
                System.out.println(String.format("File '%s' is IMAGINARY", file.getName()));
                break;
                
            default:
                log.error(String.format("Unkown type '%d' on '%s'", file.getType(), file.getName()));
                break;
        }
    }

    /**
     * Does a 'touch' command.
     * 
     * @param cmd
     * @throws java.lang.Exception
     */
    public void touch(final String[] cmd) throws Exception
    {
        if (cmd.length < 2)
        {
            throw new Exception("USAGE: touch <path>");
        }
        final FileObject file = mgr.resolveFile(cwd, cmd[1]);
        if (!file.exists())
        {
            file.createFile();
        }
        file.getContent().setLastModifiedTime(System.currentTimeMillis());
    }

    /**
     * Lists the children of a folder.
     */
    public void listChildren(final FileObject dir,
                              final boolean recursive,
                              final String prefix)
        throws FileSystemException
    {
        final FileObject[] children = dir.getChildren();
        for (int i = 0; i < children.length; i++)
        {
            final FileObject child = children[i];
            System.out.print(prefix);
            System.out.print(child.getName().getBaseName());
            if (child.getType() == FileType.FOLDER)
            {
                System.out.println("/");
                if (recursive)
                {
                    listChildren(child, recursive, prefix + "    ");
                }
            }
            else
            {
                System.out.println();
            }
        }
    }
}
