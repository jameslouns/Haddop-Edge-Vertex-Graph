import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class GeoPathLength{

    public static class DegOneTokenizer extends Mapper<Object, Text, Text, Text> {
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
                    context.write(node[1], node[0]);
                }
            }
        }
    }

    public static class DegOneReducer extends Reducer<Text, Text, Text, Text>{
        private HashMap<String,ArrayList<String>> NodeAdjList;

        @Override
        protected void setup(Context context) {
            NodeAdjList = new HashMap<String,ArrayList<String>>();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) {
            String Skey = key.toString();
            ArrayList<String> EdgeList = new ArrayList<String>();
            for(Text val : value){
                String Sval = val.toString();
                EdgeList.add(Sval);
            }
            NodeAdjList.put(Skey,EdgeList);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Map.Entry<String,ArrayList<String>> node : NodeAdjList.entrySet()){
                ArrayList temp = node.getValue();
                Iterator N = temp.iterator();
                String nextNodes="";
                while(N.hasNext()){
                    nextNodes+=","+N.next();
                }    
                context.write(new Text(node.getKey()),new Text(nextNodes));
            }
       }
    }

    public static class AdjTokenizer extends Mapper<Object, Text, Text, Text> {
        
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String KV = value.toString().replaceAll("\\s+","");
            context.write(new Text("AdjList"),new Text(KV));
        }
    }
    

    public static class DegTwoReducer extends Reducer<Text, Text, Text, IntWritable>{
        private HashMap<String,ArrayList<String>> NodeAdjList;
        private HashMap<String,Integer> CheckList, ItrList;
        private ArrayList<String> testing;

        @Override
        protected void setup(Context context) {
            NodeAdjList = new HashMap<String,ArrayList<String>>();
            ItrList = new HashMap<String,Integer>();
            testing = new ArrayList<String>();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) {
            String Skey = key.toString().trim();
            ArrayList<String> EdgeList = new ArrayList<String>();
            if(Skey.equals("AdjList")){
                for(Text val : value){
                    String[] KV = val.toString().split(",");
                    String KEY=KV[0];
                    ArrayList<String> NodeAdj = new ArrayList<String>();
                    for(int i=1;i<KV.length;i++){
                        NodeAdj.add(KV[i]);
                        String temp=KEY+","+KV[i];
                        String Rtemp=KV[i]+","+KEY;
                        if(!(ItrList.containsKey(temp)|ItrList.containsKey(Rtemp))){
                            ItrList.put(KEY+","+KV[i],1);
                        }
                    }
                    NodeAdjList.put(KEY,NodeAdj);
                }
            }else{
               NodeAdjList.put(Skey,EdgeList); 
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            CheckList = new HashMap<String,Integer>(ItrList);
            for(Map.Entry<String,Integer> node : ItrList.entrySet()){
                String CurPath = node.getKey();
                String[] SE = CurPath.split(",");

                ArrayList<String> newEnds = NodeAdjList.get(SE[1]);
                Iterator<String> newE = newEnds.iterator();
                while(newE.hasNext()){
                    String nextEnd = newE.next();
                    String newPath = SE[0]+","+nextEnd;
                    if(CheckList.containsKey(newPath)|CheckList.containsKey(nextEnd+","+SE[0])|SE[0].equals(nextEnd)){
                        continue;    
                    }
                    CheckList.put(newPath,2);
                }

                newEnds = NodeAdjList.get(SE[0]);
                newE = newEnds.iterator();
                while(newE.hasNext()){
                    String nextEnd = newE.next();
                    String newPath = SE[1]+","+nextEnd;
                    if(CheckList.containsKey(newPath)|CheckList.containsKey(nextEnd+","+SE[1])|SE[1].equals(nextEnd)){
                        continue;    
                    }
                    CheckList.put(newPath,2);
                }

            }
            for(Map.Entry<String,Integer> node : CheckList.entrySet()){
                context.write(new Text(node.getKey()),new IntWritable(node.getValue()));
            }
       }
    }

    public static class CheckListTokenizer extends Mapper<Object, Text, Text, Text> {
        
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString();
            String[] KV = val.split("\\s+");
            context.write(new Text(KV[1]),new Text(KV[0]));
        }
    }
    

    public static class DegNReducer extends Reducer<Text, Text, Text, IntWritable>{
        private HashMap<String,ArrayList<String>> NodeAdjList;
        private HashMap<String,Integer> CheckList, ItrList;
        private int maxLen;

        @Override
        protected void setup(Context context) {
            NodeAdjList = new HashMap<String,ArrayList<String>>();
            ItrList = new HashMap<String,Integer>();
            CheckList = new HashMap<String,Integer>();
            maxLen = 1;            
        }

        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) {
            String Skey = key.toString().trim();
            if(Skey.equals("AdjList")){
                for(Text val : value){
                    String[] KV = val.toString().split(",");
                    String KEY=KV[0];
                    ArrayList<String> NodeAdj = new ArrayList<String>();
                    for(int i=1;i<KV.length;i++){
                        NodeAdj.add(KV[i]);
                    }
                    NodeAdjList.put(KEY,NodeAdj);
                }                
            }else{
                int Ikey = Integer.parseInt(Skey);
                if(Ikey > maxLen){
                    ItrList.clear();
                    for(Text val : value){
                        CheckList.put(val.toString(),Ikey);
                        ItrList.put(val.toString(),Ikey);
                    }
                    maxLen=Ikey;
                }else{
                    for(Text val : value){
                        CheckList.put(val.toString(),Ikey);
                    }
                }
            } 
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Map.Entry<String,Integer> node : ItrList.entrySet()){
                String CurPath = node.getKey();
                String[] SE = CurPath.split(",");

                ArrayList<String> newEnds = NodeAdjList.get(SE[1]);
                Iterator<String> newE = newEnds.iterator();
                while(newE.hasNext()){
                    String nextEnd = newE.next();
                    String newPath = SE[0]+","+nextEnd;
                    if(CheckList.containsKey(newPath)|CheckList.containsKey(nextEnd+","+SE[0])|SE[0].equals(nextEnd)){
                        continue;    
                    }
                    CheckList.put(newPath,maxLen+1);
                }

                newEnds = NodeAdjList.get(SE[0]);
                newE = newEnds.iterator();
                while(newE.hasNext()){
                    String nextEnd = newE.next();
                    String newPath = SE[1]+","+nextEnd;
                    if(CheckList.containsKey(newPath)|CheckList.containsKey(nextEnd+","+SE[1])|SE[1].equals(nextEnd)){
                        continue;    
                    }
                    CheckList.put(newPath,maxLen+1);
                }

            }
            for(Map.Entry<String,Integer> node : CheckList.entrySet()){
                context.write(new Text(node.getKey()),new IntWritable(node.getValue()));
            }
       }
    }

    public static class LenTokenizer extends Mapper<Object, Text, Text, Text> {
        
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString();
            String[] KV = val.split("\\s+");
            context.write(new Text(KV[1]),new Text(KV[1]));
        }
    }
    
    public static class NumNodesTokenizer extends Mapper<Object, Text, Text, Text> {
        
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                context.write(new Text("0"),new Text("0"));
        }
    }

    public static class finalReducer extends Reducer<Text, Text, Text, Text>{
        private int[] numOfDegree;

        @Override
        protected void setup(Context context) {
            numOfDegree = new int[11];
            for(int i=0;i<numOfDegree.length;i++){
                numOfDegree[i]=0;
            }         
        }

        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) {
            int iKey = Integer.parseInt(key.toString());
            for(Text val : value){
                numOfDegree[iKey] += 1;
            }    
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int total=0;
            int[] Sizes=new int[11];
            for(int i=0;i<11;i++){
                Sizes[i]=Integer.toString(numOfDegree[i]).length();
                if(i>0){
                    total+=numOfDegree[i];
                }
            }
            context.write(new Text("|Category   |"+center(Sizes[1],"Dij=1")+"|"+center(Sizes[2],"Dij=2")+"|"+center(Sizes[3],"Dij=3")+"|"+center(Sizes[4],"Dij=4")+"|"+center(Sizes[5],"Dij=5")+"|"+center(Sizes[6],"Dij=6")+"|"+center(Sizes[7],"Dij=7")+"|"+center(Sizes[8],"Dij=8")+"|"+center(Sizes[9],"Dij=9")+"|"+center(Sizes[10],"Dij=10")+"|Total Shortest Pathlength|Total Number of Vertices|The Average Geodesic Path Length"),new Text(""));
            context.write(new Text("|Measurement|"+numOfDegree[1]+"|"+numOfDegree[2]+"|"+numOfDegree[3]+"|"+numOfDegree[4]+"|"+numOfDegree[5]+"|"+numOfDegree[6]+"|"+numOfDegree[7]+"|"+numOfDegree[8]+"|"+numOfDegree[9]+"|"+numOfDegree[10]+"|"+center(25,Integer.toString(total))+"|"+center(24,Integer.toString(numOfDegree[0]))+"|"+String.valueOf((2.0/(numOfDegree[0]*(numOfDegree[0]-1)))*total)),new Text(""));
        }

        protected String center(int size, String S){
            int R=S.length()+(size-S.length())/2;
            String right = String.format("%"+R+"s",S);
            return String.format("%-"+size+"s",right);
        }
    }
        public static void main(String[] args) throws Exception {
       
        Configuration conf1 = new Configuration();

        Job job1 = Job.getInstance(conf1, "DegOne");
        job1.setJarByClass(GeoPathLength.class);
        job1.setMapperClass(GeoPathLength.DegOneTokenizer.class);
        job1.setReducerClass(GeoPathLength.DegOneReducer.class);
        job1.setNumReduceTasks(1);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/One"));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf1, "DegTwo");
        job2.setJarByClass(GeoPathLength.class);
        job2.setMapperClass(GeoPathLength.AdjTokenizer.class);
        job2.setReducerClass(GeoPathLength.DegTwoReducer.class);
        job2.setNumReduceTasks(1);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]+"/One"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/Two"));
        job2.waitForCompletion(true);

        Job job3 = Job.getInstance(conf1, "DegThree");
        job3.setJarByClass(GeoPathLength.class);
        MultipleInputs.addInputPath(job3 ,new Path(args[1]+"/One"), TextInputFormat.class, GeoPathLength.AdjTokenizer.class);
        MultipleInputs.addInputPath(job3 ,new Path(args[1]+"/Two"), TextInputFormat.class, GeoPathLength.CheckListTokenizer.class);
        job3.setReducerClass(GeoPathLength.DegNReducer.class);
        job3.setNumReduceTasks(1);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job3, new Path(args[1]+"/Three"));
        job3.waitForCompletion(true);

        Job job4 = Job.getInstance(conf1, "DegFour");
        job4.setJarByClass(GeoPathLength.class);
        MultipleInputs.addInputPath(job4 ,new Path(args[1]+"/One"), TextInputFormat.class, GeoPathLength.AdjTokenizer.class);
        MultipleInputs.addInputPath(job4 ,new Path(args[1]+"/Three"), TextInputFormat.class, GeoPathLength.CheckListTokenizer.class);
        job4.setReducerClass(GeoPathLength.DegNReducer.class);
        job4.setNumReduceTasks(1);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job4, new Path(args[1]+"/Four"));
        job4.waitForCompletion(true);

        Job job5 = Job.getInstance(conf1, "DegFive");
        job5.setJarByClass(GeoPathLength.class);
        MultipleInputs.addInputPath(job5 ,new Path(args[1]+"/One"), TextInputFormat.class, GeoPathLength.AdjTokenizer.class);
        MultipleInputs.addInputPath(job5 ,new Path(args[1]+"/Four"), TextInputFormat.class, GeoPathLength.CheckListTokenizer.class);
        job5.setReducerClass(GeoPathLength.DegNReducer.class);
        job5.setNumReduceTasks(1);
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job5, new Path(args[1]+"/Five"));
        job5.waitForCompletion(true);

        Job job6 = Job.getInstance(conf1, "DegSix");
        job6.setJarByClass(GeoPathLength.class);
        MultipleInputs.addInputPath(job6 ,new Path(args[1]+"/One"), TextInputFormat.class, GeoPathLength.AdjTokenizer.class);
        MultipleInputs.addInputPath(job6 ,new Path(args[1]+"/Five"), TextInputFormat.class, GeoPathLength.CheckListTokenizer.class);
        job6.setReducerClass(GeoPathLength.DegNReducer.class);
        job6.setNumReduceTasks(1);
        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(Text.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job6, new Path(args[1]+"/Six"));
        job6.waitForCompletion(true);

        Job job7 = Job.getInstance(conf1, "DegSeven");
        job7.setJarByClass(GeoPathLength.class);
        MultipleInputs.addInputPath(job7 ,new Path(args[1]+"/One"), TextInputFormat.class, GeoPathLength.AdjTokenizer.class);
        MultipleInputs.addInputPath(job7 ,new Path(args[1]+"/Six"), TextInputFormat.class, GeoPathLength.CheckListTokenizer.class);
        job7.setReducerClass(GeoPathLength.DegNReducer.class);
        job7.setNumReduceTasks(1);
        job7.setMapOutputKeyClass(Text.class);
        job7.setMapOutputValueClass(Text.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job7, new Path(args[1]+"/Seven"));
        job7.waitForCompletion(true);

        Job job8 = Job.getInstance(conf1, "DegEight");
        job8.setJarByClass(GeoPathLength.class);
        MultipleInputs.addInputPath(job8 ,new Path(args[1]+"/One"), TextInputFormat.class, GeoPathLength.AdjTokenizer.class);
        MultipleInputs.addInputPath(job8 ,new Path(args[1]+"/Seven"), TextInputFormat.class, GeoPathLength.CheckListTokenizer.class);
        job8.setReducerClass(GeoPathLength.DegNReducer.class);
        job8.setNumReduceTasks(1);
        job8.setMapOutputKeyClass(Text.class);
        job8.setMapOutputValueClass(Text.class);
        job8.setOutputKeyClass(Text.class);
        job8.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job8, new Path(args[1]+"/Eight"));
        job8.waitForCompletion(true);

        Job job9 = Job.getInstance(conf1, "DegNine");
        job9.setJarByClass(GeoPathLength.class);
        MultipleInputs.addInputPath(job9 ,new Path(args[1]+"/One"), TextInputFormat.class, GeoPathLength.AdjTokenizer.class);
        MultipleInputs.addInputPath(job9 ,new Path(args[1]+"/Eight"), TextInputFormat.class, GeoPathLength.CheckListTokenizer.class);
        job9.setReducerClass(GeoPathLength.DegNReducer.class);
        job9.setNumReduceTasks(1);
        job9.setMapOutputKeyClass(Text.class);
        job9.setMapOutputValueClass(Text.class);
        job9.setOutputKeyClass(Text.class);
        job9.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job9, new Path(args[1]+"/Nine"));
        job9.waitForCompletion(true);

        Job job10 = Job.getInstance(conf1, "DegTen");
        job10.setJarByClass(GeoPathLength.class);
        MultipleInputs.addInputPath(job10 ,new Path(args[1]+"/One"), TextInputFormat.class, GeoPathLength.AdjTokenizer.class);
        MultipleInputs.addInputPath(job10 ,new Path(args[1]+"/Nine"), TextInputFormat.class, GeoPathLength.CheckListTokenizer.class);
        job10.setReducerClass(GeoPathLength.DegNReducer.class);
        job10.setNumReduceTasks(1);
        job10.setMapOutputKeyClass(Text.class);
        job10.setMapOutputValueClass(Text.class);
        job10.setOutputKeyClass(Text.class);
        job10.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job10, new Path(args[1]+"/Ten"));
        job10.waitForCompletion(true);

        Job job11 = Job.getInstance(conf1, "final");
        job11.setJarByClass(GeoPathLength.class);
        MultipleInputs.addInputPath(job11 ,new Path(args[1]+"/One"), TextInputFormat.class, GeoPathLength.NumNodesTokenizer.class);
        MultipleInputs.addInputPath(job11 ,new Path(args[1]+"/Ten"), TextInputFormat.class, GeoPathLength.LenTokenizer.class);
        job11.setReducerClass(GeoPathLength.finalReducer.class);
        job11.setNumReduceTasks(1);
        job11.setMapOutputKeyClass(Text.class);
        job11.setMapOutputValueClass(Text.class);
        job11.setOutputKeyClass(Text.class);
        job11.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job11, new Path(args[1]+"/final"));


        System.exit(job11.waitForCompletion(true) ? 0 : 1);
    }
}
