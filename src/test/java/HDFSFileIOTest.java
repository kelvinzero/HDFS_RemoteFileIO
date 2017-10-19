import hdfs.HDFSConnection;
import hdfs.HDFSFileIO;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

public class HDFSFileIOTest {

    private HDFSFileIO hdfs;

    @Before
    public void instantiateHDFSFileIO() throws Exception{
        HDFSConnection conn = new HDFSConnection("hdfs", "hdfs://192.168.1.110", 8020, "fs.defaultFS");
        hdfs = new HDFSFileIO(conn, "kelvinzero");
    }

    @Test
    public void writeFileAsUser() throws Exception {
        hdfs.writeFileAsUser("C:\\Users\\Josh Cotes\\IdeaProjects\\ClouderaHDFStester\\.idea\\libraries\\Maven__asm_asm_3_1.xml", "/storage");
        hdfs.deleteAsUser("/storage/Maven__asm_asm_3_1.xml");
    }

    @Test
    public void writeFile() throws Exception {
       hdfs.writeFile("C:\\Users\\Josh Cotes\\IdeaProjects\\ClouderaHDFStester\\.idea\\libraries\\Maven__asm_asm_3_1.xml", "/user/kelvinzero/storage");
       hdfs.delete("/user/kelvinzero/storage/Maven__asm_asm_3_1.xml");
    }

    @Test
    public void writeLargerFile() throws Exception{
        hdfs.writeFileAsUser("C:\\Users\\Josh Cotes\\Downloads/QuickTimeInstaller.exe", "/storage/quicktime");
        hdfs.deleteAsUser("/storage/quicktime/QuickTimeInstaller.exe");
        hdfs.deleteAsUser("/storage/quicktime");
    }

    @Test
    public void writeReadLargerFile() throws Exception{
        hdfs.writeFileAsUser("C:\\Users\\Josh Cotes\\Downloads\\QuickTimeInstaller.exe", "/storage/quicktime");

        hdfs.readFileAsUser("/storage/quicktime/QuickTimeInstaller.exe", "C:\\testdownload");
        File downloaded = new File("C:\\testdownload\\QuickTimeInstaller.exe");
        Assert.assertTrue(downloaded.exists());
        hdfs.deleteAsUser("/storage/quicktime/QuickTimeInstaller.exe");
        hdfs.deleteAsUser("/storage/quicktime");
        downloaded.delete();
    }

    @Test
    public void directoryList() throws Exception {
        System.out.println();
       Arrays.stream(hdfs.directoryList("/user/kelvinzero")).forEach(System.out::println);
    }

    @Test
    public void readFile() throws Exception{
        hdfs.writeFileAsUser("C:\\Users\\Josh Cotes\\IdeaProjects\\ClouderaHDFStester\\.idea\\libraries\\Maven__asm_asm_3_1.xml", "/storage");
        hdfs.readFileAsUser("/storage/Maven__asm_asm_3_1.xml", "C:\\testdownload");
        File downloaded = new File("C:\\testdownload\\Maven__asm_asm_3_1.xml");
        Assert.assertTrue(downloaded.exists());
        hdfs.delete("/user/kelvinzero/storage/Maven__asm_asm_3_1.xml");
        downloaded.delete();
    }
    @After
    public void closeSystem() throws Exception{
        hdfs.close();
    }
}