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

public class Friends {
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
    public static class FriendReducer extends Reducer<Text, Text, Text, Text>{
        private HashMap<String,ArrayList<String>> Out;
        private  ArrayList<String> temp,Friend;
@Override
protected void setup(Context context) {
    Friend = new ArrayList<String>();
    Out = new HashMap<String,ArrayList<String>>();
}

@Override
protected void reduce(Text key, Iterable<Text> value, Context context) {
    for(Text val : value){
    String Svalue = val.toString();
    String Skey = key.toString();
    if(Out.containsKey(Skey)){
        temp=Out.get(Skey);
        temp.add(Svalue);
        Out.replace(Skey,temp);
    }else{
        temp = new ArrayList<String>();
        temp.add(Svalue);
        Out.put(Skey,temp);
    }
    if(Out.containsKey(Svalue)){
        if(Out.get(Svalue).contains(Skey)){
            Friend.add(Svalue+" "+Skey);
            Friend.add(Skey+" "+Svalue);
        }
    }
}
}
@Override
protected void cleanup(Context context) throws IOException, InterruptedException {
    Collections.sort(Friend);
    for(int i=0;i<100;i++){
        String currentUsr=Friend.get(i);
        context.write(new Text(currentUsr),new Text("")); 
    }
}
}
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "Friends new");
job.setJarByClass(Friends.class);
job.setMapperClass(Friends.TokenizerMapper.class);
job.setReducerClass(Friends.FriendReducer.class);
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
