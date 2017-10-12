import org.junit.Before;

public class ClouderaAccessTest {

    HDFSFileIO hdfs;

    @Before
    public void instantiateHDFSFileIO() throws Exception{
        hdfs = new HDFSFileIO("kelvinzero", "192.168.1.110", 8020);
    }

    @org.junit.Test
    public void testConnection() throws Exception {
        hdfs.writeToHDFS("C:\\Users\\Josh Cotes\\Downloads\\torbrowser-install-7.0.5_en-US.exe", "/storage");
    }

}