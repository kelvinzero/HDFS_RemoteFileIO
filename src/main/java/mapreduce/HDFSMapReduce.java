package mapreduce;

import hdfs.C_MapperOne;
import hdfs.C_ReducerOne;
import hdfs.HDFSConnection;
import jdk.nashorn.tools.Shell;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class HDFSMapReduce {


    public static void main(String[] args) throws Exception{
        Configuration conf;
        HDFSConnection conn = new HDFSConnection("hdfs", "yarn://192.168.1.110:8021", 8021, "mapred.job.tracker");
        conf = conn.getConfiguration();
       conf.set("fs.defaultFS", "hdfs://192.168.1.110:8020");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("yarn.resourcemanager.address", "192.168.1.110:8032");
        conf.set("mapred.job.tracker", "192.168.1.110:8021");
        conf.set("mapred.job.name", "myapp");
        conf.set("yarn.app.mapreduce.am.resource.mb", "1024");
        //conf.set("yarn.resourcemanager.address", "192.168.1.110:8050"); // see step 3
        conf.set("mapreduce.framework.name", "yarn");
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("mapred.remote.os", "Windows");
        conf.set("mapreduce.map.class", "C_MapperOne.class");
        conf.set("mapreduce.reduce.class", "C_ReducerOne.class");

        //conf.set("fs.defaultFS", "hdfs://<your-hostname>/"); // see step 2
        try
        {
            JobConf jcf = new JobConf();
            JobClient jcl = new JobClient(jcf);
            jcf.set("fs.defaultFS", "hdfs://192.168.1.110:8020");
            jcf.set("yarn.resourcemanager.address", "192.168.1.110:8032");
            jcf.set("mapred.job.tracker", "192.168.1.110:8021");
            jcf.set("mapreduce.framework.name", "yarn");

            jcf.setMapperClass(C_MapperOne.class);


            Job job = Job.getInstance(conf);

            // here you have to put your mapper class
            job.setMapperClass(C_MapperOne.class);

            // here you have to put your reducer class
            job.setReducerClass(C_ReducerOne.class);

            // here you have to set the jar which is containing your
            // map/reduce class, so you can use the mapper class
            job.setJarByClass(C_MapperOne.class);

            // key/value of your reducer output
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // this is setting the format of your input, can be TextInputFormat
            job.setInputFormatClass(SequenceFileInputFormat.class);
            // same with output
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setMapOutputKeyClass(TextOutputFormat.class);
            job.setMapOutputValueClass(TextOutputFormat.class);

            // here you can set the path of your input
            FileInputFormat.addInputPath(job, new Path("/user/kelvinzero/storage/climate_data/1921.csv"));

            // this deletes possible output paths to prevent job failures
            FileSystem fs = FileSystem.get(conf);
            Path out = new Path("/user/kelvinzero/storage/climate_data/processed");
            fs.delete(out, false);


            // finally set the empty out path
            FileOutputFormat.setOutputPath(job, out);

            // this waits until the job completes and prints debug out to STDOUT or whatever
            // has been configured in your log4j properties.
            job.waitForCompletion(true);

        }catch(IOException e){
            e.printStackTrace();
        }
        catch (InterruptedException c){
            c.printStackTrace();
        }
        catch (ClassNotFoundException d){
            d.printStackTrace();
        }
    }

}
