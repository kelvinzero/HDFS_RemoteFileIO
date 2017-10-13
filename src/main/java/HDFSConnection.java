import org.apache.hadoop.conf.Configuration;

public class HDFSConnection {

    private Configuration mConfig;
    private String mHDFSURI;
    private String mSiteFile;

    /**
     * Creates the settings needed to access the cluster and HDFS filesystem.
     * @param HDFSurl   the IP or URL of the HDFS namenode
     * @param HDFSport  the port of the HDFS namenode
     */
    HDFSConnection(String HDFSurl, int HDFSport, String siteFile){

        mSiteFile = siteFile;
        mHDFSURI = "hdfs://" + HDFSurl + ":" + HDFSport;
        resetConfiguration();
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");
    }

    private void resetConfiguration(){
        mConfig = new Configuration();
        mConfig.set(mSiteFile, mHDFSURI);
    }

    /**
     * Gets the HDFS configuration.
     * @return the configuration
     */
    public Configuration getConfiguration(){
        return mConfig;
    }

    /**
     * Gets the HDFS URI.
     * @return the URI
     */
    public String getURI(){
        return mHDFSURI;
    }

    /**
     * Sets the uri and resets configurations.
     * @param uri   the uri
     * @param port  the port number
     */
    public void setURI(String uri, int port) {
        mHDFSURI = "hdfs://" + uri + ":" + port;
        resetConfiguration();
    }
}