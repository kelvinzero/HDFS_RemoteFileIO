import org.junit.Before;

public class ClouderaAccessTest {

    private HDFSFileIO hdfs;

    @Before
    public void instantiateHDFSFileIO() throws Exception{
        hdfs = new HDFSFileIO("kelvinzero", "192.168.1.110", 8020);
    }

    @org.junit.Test
    public void testConnection() throws Exception {
        hdfs.writeToHDFS("C:\\Users\\Josh Cotes\\Downloads\\debian-9.1.0-amd64-netinst.iso", "/storage");
    }

}