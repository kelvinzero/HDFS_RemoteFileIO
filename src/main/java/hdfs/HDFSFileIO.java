package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;

public class HDFSFileIO {

    private Configuration mConfig;
    private FileSystem mFileSystem;
    private String mHDFSuser;
    private String mHDFSuserHome;

    /**
     * Creates the settings needed to access the cluster and HDFS filesystem.
     * @param HDFSuser  the user to operate as
     * @throws IOException  if uri or port is incorrect
     */
    public HDFSFileIO(HDFSConnection connection, String HDFSuser) throws IOException{
        mConfig = connection.getConfiguration();
        mHDFSuser = HDFSuser;
        mHDFSuserHome = "/user/" + mHDFSuser;
        mConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        mConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        mFileSystem = FileSystem.get(URI.create(connection.getURI()), mConfig);
    }

    /**
     * Reads a file from the HDFS over the network and writes it to the local file system. Local directory
     * is created if it doesn't exist. Read path begins at root.
     * @param readPathHDFS  path to the file in HDFS
     * @param writePathLocal    local write path
     * @throws IOException  Throws if local file exists or the HDFS file doesn't exist
     */
    public void readFile(String readPathHDFS, String writePathLocal) throws IOException{

        if(!mFileSystem.exists(new Path(readPathHDFS)))
            throw new IOException("The file: " + readPathHDFS + " doesn't exist.");

        String[] splitReadPath = readPathHDFS.split("\\\\|/");
        String fileName = splitReadPath[splitReadPath.length-1];
        File inFile = new File(writePathLocal + "\\" + fileName);

        if(inFile.isFile())
            throw new IOException("File: " + writePathLocal + "\\" + fileName + " already exists.");

        if(!inFile.isDirectory())
            inFile.getParentFile().mkdirs();

        FSDataInputStream inputStream = mFileSystem.open(new Path(readPathHDFS));
        OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(inFile));
        long fileLength = mFileSystem.listStatus(new Path(readPathHDFS))[0].getLen();

        System.out.println("Copying file:  " + readPathHDFS + "   ==>   " + writePathLocal);
        streamTransfer(inputStream, outputStream, fileLength);
        inputStream.close();
        outputStream.close();
        System.out.println("Successfully copied: " + fileName);
    }

    /**
     * Writes from a local file to HDFS over the network. Creates a home directory for the userHDFS if one
     * doesn't exist. Creates the destination path if it doesn't exist.
     * @param writePathHDFS the write path in hdfs
     * @throws IOException if file exists
     */
    public void writeFile(String localReadPath, String writePathHDFS) throws IOException {

        if(!mFileSystem.exists(new Path(writePathHDFS)))
            mFileSystem.mkdirs(new Path(writePathHDFS));

        String[] splitPath = localReadPath.split("\\\\|/");
        String outPathHDFS = writePathHDFS + "/" + splitPath[splitPath.length-1];

        if(mFileSystem.exists(new Path(outPathHDFS)))
            throw new IOException("The file " + outPathHDFS + " already exists. Please delete the file first or append to.");

        InputStream inputStream = new BufferedInputStream(new FileInputStream(localReadPath));
        FSDataOutputStream outputStream = mFileSystem.create(new Path(outPathHDFS));
        long fileLength = new File(localReadPath).length();

        System.out.println("Copying file:  " + localReadPath + "   ==>   " + outPathHDFS);
        streamTransfer(inputStream, outputStream, fileLength);
        inputStream.close();
        outputStream.close();
        System.out.println("Successfully copied: " + splitPath[splitPath.length-1]);
    }

    /**
     * Reads a file from the HDFS over the network and writes it to the local file system. Local directory
     * is created if it doesn't exist. Read path is on top of the user's home directory.
     * @param readPathHDFS  path to the file in HDFS
     * @param writePathLocal    local write path
     * @throws IOException  Throws if local file exists or the HDFS file doesn't exist
     */
    public void readFileAsUser(String readPathHDFS, String writePathLocal) throws IOException{
        readFile(mHDFSuserHome + readPathHDFS, writePathLocal);
    }

    /**
     * Writes from a local file to HDFS over the network. Creates a home directory for the userHDFS if one
     * doesn't exist. Create destination path if doesn't exist. Write path is written on top of the user's
     * home directory.
     * @param writePathHDFS the write path in hdfs
     * @throws IOException if file exists
     */
    public void writeFileAsUser(String localReadPath, String writePathHDFS) throws IOException{

        mFileSystem.setWorkingDirectory(new Path("/user/" + mHDFSuser));

        if(!mFileSystem.exists(new Path(mHDFSuserHome)))
            mFileSystem.mkdirs(new Path(mHDFSuserHome));

        writeFile(localReadPath, mHDFSuserHome + writePathHDFS);
    }

    /**
     * Performs a stream data transfer while indicating * for every 2% transfered.
     * @param is    input stream
     * @param os    output stream
     * @param fileLength    the length of the file being transfered
     * @throws IOException  Throws for file IO errors
     */
    private void streamTransfer(InputStream is, OutputStream os, long fileLength) throws IOException{

        long newMarker = fileLength/50;
        long bytesWritten = 0;
        int numBytesRead;
        byte[] readByte = new byte[1024];

        System.out.print("[");
        while((numBytesRead = is.read(readByte)) > 0){
            os.write(readByte, 0, numBytesRead);
            bytesWritten += numBytesRead;
            if(bytesWritten >= newMarker){
                System.out.print("*");
                bytesWritten = 0;
            }
        }
        System.out.println("]");
    }

    /**
     * Returns an array of FileStatus containing informations about items in the path on top of the user directory.
     * @param path  The hdfs path to list
     * @return  The FileStatus array of directory contents
     * @throws IOException  Throws if the path doesn't exist
     */
    public FileStatus[] directoryList(final String path) throws IOException {

        Path filePath = new Path(path);
        if(!mFileSystem.exists(filePath))
            throw new IOException("Directory: " + path + "doesn't exist.");
        return mFileSystem.listStatus(filePath);
    }

    /**
     * Returns an array of FileStatus containing informations about items in the path on top of the user directory.
     * @param path  The hdfs path to list
     * @return  The FileStatus array of directory contents
     * @throws IOException  Throws if the path doesn't exist
     */
    public FileStatus[] directoryListAsUser(final String path) throws IOException {
        return directoryList(mHDFSuserHome + path);
    }

    /**
     * Creates a directory in the HDFS on top of the user directory.
     * @param path  The HDFS directory path to create
     * @throws IOException  Throws for incorrect privileges and incorrect paths
     */
    public void directoryCreateAsUser(final String path) throws IOException {
        directoryCreate(mHDFSuserHome + path);
    }

    /**
     * Creates a directory in the HDFS from the root directory.
     * @param path  The HDFS directory path to create
     * @throws IOException  Throws for incorrect privileges and incorrect paths
     */
    public void directoryCreate(final String path) throws IOException {
        Path filePath = new Path(path);
        if(mFileSystem.exists(filePath))
            throw new IOException("Directory: " + path + " already exists.");
        mFileSystem.mkdirs(filePath);
    }

    /**
     * Removes a path or directory from the HDFS starting from root directory.
     * @param path  The HDFS path to delete
     * @throws IOException  Throws for non-existent directory or file
     */
    public void delete(final String path) throws IOException {

        Path filePath = new Path(path);
        if(!mFileSystem.exists(filePath))
            throw new IOException("Path to remove: " + path + " doesn't exist.");
        mFileSystem.delete(filePath, false);
        System.out.println("Successfully deleted " + path);
    }

    /**
     * Removes a path or directory from the HDFS on top of user directory.
     * @param path  The HDFS path to delete
     * @throws IOException  Throws for non-existent directory or file
     */
    public void deleteAsUser(final String path) throws IOException {
        delete(mHDFSuserHome + path);
    }

    /**
     * Closes the HDFS filesystem interface.
     * @throws IOException  Throws if filesystem configuration is incorrect or not connected.
     */
    public void close() throws IOException{
        mFileSystem.close();
    }
}

