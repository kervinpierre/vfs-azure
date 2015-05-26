# vfs-azure
Azure provider for Apache Commons VFS - http://commons.apache.org/proper/commons-vfs/

From the website...
"Commons VFS provides a single API for accessing various different file systems. It presents a uniform view of the files from various different sources, such as the files on local disk, on an HTTP server, or inside a Zip archive."

Now Apache Commons VFS can now add Azure Blob Storage to the list

The project also includes a small shell ( taken from the original Apache Commons VFS tests ) but improved with JLine2 ( for command completion and history ).  This should allow interactive testing.

Here is an example using the API
```java
// Grab some credentials. The "testProperties" class is just a regular properties class
// Retrieve the properties however you please
String currAccountStr = testProperties.getProperty("azure.account.name"); // .blob.core.windows.net
String currKey = testProperties.getProperty("azure.account.key");
String currContainerStr = testProperties.getProperty("azure.test0001.container.name");
String currHost = testProperties.getProperty("azure.host");  // <account>.blob.core.windows.net
String currFileNameStr;

// Now let's create a temp file just for upload
File temp = File.createTempFile("uploadFile01", ".tmp");
try(FileWriter fw = new FileWriter(temp))
{
    BufferedWriter bw = new BufferedWriter(fw);
    bw.append("testing...");
    bw.flush();
}

// Create an Apache Commons VFS manager option and add 2 providers. Local file and Azure.
// All done programmatically
DefaultFileSystemManager currMan = new DefaultFileSystemManager();
currMan.addProvider(AzConstants.AZSBSCHEME, new AzFileProvider());
currMan.addProvider("file", new DefaultLocalFileProvider());
currMan.init(); 

// Create a new Authenticator for the credentials
StaticUserAuthenticator auth = new StaticUserAuthenticator("", currAccountStr, currKey);
FileSystemOptions opts = new FileSystemOptions(); 
DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth); 

// Create a URL for creating this remote file
currFileNameStr = "test01.tmp";
String currUriStr = String.format("%s://%s/%s/%s", 
                   AzConstants.AZSBSCHEME, currHost, currContainerStr, currFileNameStr);

// Resolve the imaginary file remotely.  So we have a file object
FileObject currFile = currMan.resolveFile(currUriStr, opts);

// Resolve the local file for upload
FileObject currFile2 = currMan.resolveFile(
        String.format("file://%s", temp.getAbsolutePath()));

// Use the API to copy from one local file to the remote file 
currFile.copyFrom(currFile2, Selectors.SELECT_SELF);

// Delete the temp we don't need anymore
temp.delete();
```


