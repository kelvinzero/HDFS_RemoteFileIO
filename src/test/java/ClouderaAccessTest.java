import org.junit.Before;

public class ClouderaAccessTest {

    private HDFSFileIO hdfs;

    @Before
    public void instantiateHDFSFileIO() throws Exception{
        hdfs = new HDFSFileIO("kelvinzero", "192.168.1.110", 8020);
    }

    @org.junit.Test
    public void testConnection() throws Exception {
        hdfs.writeFileHDFS("C:\\Users\\Josh Cotes\\Downloads\\android-studio-bundle-162.4069837-windows.exe", "/storage");
    }

}