import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;

public class EdgeVertexCount{
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text[] node = new Text[2];
        private int count;
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            count=0;
            while (itr.hasMoreTokens()) {
                String s = itr.nextToken();
                node[count] = new Text( s );
                count++;
                if(count==2){
                    count=0;
                    context.write(node[0], node[1]);
                }
            }
        }
    }
    public static class CountReducer extends Reducer<Text, Text, Text, Text>{
        private HashMap<String,Integer> Vertex;
        private int EdgeCount;
@Override
protected void setup(Context context) {
    Vertex = new HashMap<String,Integer>();
    EdgeCount = 0;
}

@Override
protected void reduce(Text key, Iterable<Text> value, Context context) {
    for(Text val : value){
        String Svalue = val.toString();
        String Skey = key.toString();
        Vertex.put(Skey,1);
        Vertex.put(Svalue,1);
        EdgeCount++;
    }
}
@Override
protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("Vertex Count"),new Text("Edge Count"));
        context.write(new Text(Integer.toString(Vertex.size())), new Text(Integer.toString(EdgeCount)));
    }
}

public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "Count new");
job.setJarByClass(EdgeVertexCount.class);
job.setMapperClass(EdgeVertexCount.TokenizerMapper.class);
job.setReducerClass(EdgeVertexCount.CountReducer.class);
job.setNumReduceTasks(1);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(Text.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
