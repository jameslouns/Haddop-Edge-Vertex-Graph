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

public class InOutDegree {
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
    public static class InReducer extends Reducer<Text, Text, Text, Text>{
        private HashMap<String,Integer> In;
        private int temp;
@Override
protected void setup(Context context) {
    In = new HashMap<String,Integer>();
}

@Override
protected void reduce(Text key, Iterable<Text> value, Context context) {
    String Skey = key.toString();
    for(Text val: value){
        String Svalue = val.toString();
        if(In.get(Svalue)==null){
            In.put(Svalue,1);
        }else{
            temp=In.remove(Svalue);
            temp++;
            In.put(Svalue,temp);
        }
    }
}
@Override
protected void cleanup(Context context) throws IOException, InterruptedException {
    ArrayList<String> InList = new ArrayList<String>();
    ArrayList<String> NewValues = new ArrayList<String>();
    int CurrentMax;
    for(int i=0;i<100;i++){
        CurrentMax=0;
        NewValues.clear();
        for(Map.Entry<String,Integer> node: In.entrySet()){
            if(node.getValue()>CurrentMax){
                CurrentMax=node.getValue();
                NewValues.clear();
                NewValues.add(node.getKey());
            }else if(node.getValue()==CurrentMax){
                NewValues.add(node.getKey());
            }
        }
        Collections.sort(NewValues);
        NewValues.forEach(node -> InList.add(node+" "+Integer.toString(In.get(node))));
        NewValues.forEach(node -> In.remove(node));
        i+=(NewValues.size()-1);
    }
    for(int i=0;i<100;i++){
        context.write(new Text(InList.get(i)),new Text(""));
    }
}
}

    public static class OutReducer extends Reducer<Text, Text, Text, Text>{
        private HashMap<String,Integer> In,Out;
        private int temp;
@Override
protected void setup(Context context) {
    Out = new HashMap<String,Integer>();
}

@Override
protected void reduce(Text key, Iterable<Text> value, Context context) {
    String Skey = key.toString();
    for(Text val: value){
        if(Out.get(Skey)==null){
            Out.put(Skey,1);
        }else{
            temp=Out.remove(Skey);
            temp++;
            Out.put(Skey,temp);
        }
    }
}
@Override
protected void cleanup(Context context) throws IOException, InterruptedException {
    ArrayList<String> OutList = new ArrayList<String>();
    ArrayList<String> NewValues = new ArrayList<String>();
    int CurrentMax;
    for(int i=0;i<100;i++){
        CurrentMax=0;
        NewValues.clear();
        for(Map.Entry<String,Integer> node: Out.entrySet()){
            if(node.getValue()>CurrentMax){
                CurrentMax=node.getValue();
                NewValues.clear();
                NewValues.add(node.getKey());
            }else if(node.getValue()==CurrentMax){
                NewValues.add(node.getKey());
            }
        }
        Collections.sort(NewValues);
        NewValues.forEach(node -> OutList.add(node+" "+Integer.toString(Out.get(node))));
        NewValues.forEach(node -> Out.remove(node));
        i+=(NewValues.size()-1);
    }
    for(int i=0;i<100;i++){
        context.write(new Text(OutList.get(i)),new Text(""));
    }
}
}

public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "IN new");
job.setJarByClass(InOutDegree.class);
job.setMapperClass(InOutDegree.TokenizerMapper.class);
job.setReducerClass(InOutDegree.InReducer.class);
job.setNumReduceTasks(1);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(Text.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]+"/IN"));
job.waitForCompletion(true);

Job job2 = Job.getInstance(conf, "Out new");
job2.setJarByClass(InOutDegree.class);
job2.setMapperClass(InOutDegree.TokenizerMapper.class);
job2.setReducerClass(InOutDegree.OutReducer.class);
job2.setNumReduceTasks(1);
job2.setMapOutputKeyClass(Text.class);
job2.setMapOutputValueClass(Text.class);
job2.setOutputKeyClass(Text.class);
job2.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(job2, new Path(args[0]));
FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/OUT"));
System.exit(job2.waitForCompletion(true) ? 0 : 1);

}
}
