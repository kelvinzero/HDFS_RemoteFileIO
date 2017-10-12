

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import javax.swing.*;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

public class HDFSFileIO {

    private static final Logger logger = Logger.getLogger("io.saagie.example.hdfs.Main");
    private Configuration mConfig;
    private FileSystem mFileSystem;
    private String mHDFSuri;
    private String mHDFSuser;

    /**
     * Creates the settings needed to access the cluster and HDFS filesystem.
     * @param HDFSuser  the user to operate as
     * @param HDFSurl   the IP or URL of the HDFS namenode
     * @param HDFSport  the port of the HDFS namenode
     * @throws IOException  if uri or port is incorrect
     */
    public HDFSFileIO(String HDFSuser, String HDFSurl, int HDFSport) throws IOException{

        String hdfsURIstring = "hdfs://" + HDFSurl + ":" + HDFSport;
        mHDFSuser = HDFSuser;
        mConfig = new Configuration();
        mConfig.set("fs.defaultFS", hdfsURIstring);
        mConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        mConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");
        mFileSystem = FileSystem.get(URI.create(hdfsURIstring), mConfig);
    }

    /**
     * Writes from a local file to HDFS over the network. Creates a home directory for the userHDFS if one
     * doesn't exist. Create destination path if doesn't exist. Write path is written on top of the user's
     * home directory.
     * @param writePathHDFS the write path in hdfs
      * @throws IOException
     */
    public void writeToHDFS(String localReadPath, String writePathHDFS) throws IOException{

        // create a home directory for the username if doesn't exist
        String userHomePath = "/user/" + mHDFSuser;
        mFileSystem.setWorkingDirectory(new Path("/user/" + mHDFSuser));
        if(!mFileSystem.exists(new Path(userHomePath)))
            mFileSystem.mkdirs(new Path(userHomePath));

        // create the destination folder if doesnt exist
        if(!mFileSystem.exists(new Path(userHomePath + writePathHDFS)))
            mFileSystem.mkdirs(new Path(userHomePath + writePathHDFS));

        // check if the file already exists in HDFS
        String[] splitPath = localReadPath.split("\\\\|/");
        String outPathHDFS = userHomePath + writePathHDFS + "/" + splitPath[splitPath.length-1];
        if(mFileSystem.exists(new Path(outPathHDFS)))
            throw new IOException("The file " + outPathHDFS + " already exists. Please delete the file first or append to.");

        // create input and output streams to copy files from local to HDFS
        InputStream is = new BufferedInputStream(new FileInputStream(localReadPath));
        FSDataOutputStream outputStream = mFileSystem.create(new Path(outPathHDFS), new Progressable() {
            public void progress() {
                System.out.print("*");
            }
        });

        System.out.print("\n[");
        IOUtils.copyBytes(is, outputStream, mConfig);
        System.out.println("]");
    }
}

