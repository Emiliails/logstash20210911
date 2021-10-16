import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class RecruitReducer extends Reducer<Text, IntWritable,MySqlWritable, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, MySqlWritable, NullWritable>.Context context) throws IOException, InterruptedException {
        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()){
            count += iterator.next().get();
        }
        context.write(new MySqlWritable(new CompanyNameCount(key.toString(),count)),NullWritable.get());
    }
}
