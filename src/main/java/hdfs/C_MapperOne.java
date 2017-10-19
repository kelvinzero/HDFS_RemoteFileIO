package hdfs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.StringTokenizer;

public class C_MapperOne implements org.apache.hadoop.mapred.Mapper {

    private Text word = new Text();

    @Override
    public void map(Object o, Object o2, OutputCollector outputCollector, Reporter reporter) throws IOException {

        StringTokenizer itr = new StringTokenizer(o2.toString(),",");

        while (itr.hasMoreTokens())
        {
            word.set(itr.nextToken().trim());
            outputCollector.collect(o, word);
        }
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf jobConf) {

    }
}
