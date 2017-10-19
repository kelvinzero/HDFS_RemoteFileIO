package hdfs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class C_ReducerOne extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException
    {
        String translations = "";
        result.set(translations);
        context.write(key, result);
    }

}
