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

public class ClusterCoefficient{

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
    

    public static class DegTwoReducer extends Reducer<Text, Text, Text, Text>{
        private HashMap<String,ArrayList<String>> NodeAdjList;
        private HashMap<String,Integer> CheckList, ItrList;
        private int numTri;
        @Override
        protected void setup(Context context) {
            NodeAdjList = new HashMap<String,ArrayList<String>>();
            ItrList = new HashMap<String,Integer>();
            numTri=0;
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
            CheckList = new HashMap<String,Integer>();
            for(Map.Entry<String,Integer> node : ItrList.entrySet()){
                String CurPath = node.getKey();
                String[] SE = CurPath.split(",");

                ArrayList<String> newEnds = NodeAdjList.get(SE[1]);
                Iterator<String> newE = newEnds.iterator();
                while(newE.hasNext()){
                    String nextEnd = newE.next();
                    String newPath = SE[0]+","+SE[1]+","+nextEnd;
                    if(CheckList.containsKey(newPath)|CheckList.containsKey(nextEnd+","+SE[1]+","+SE[0])|SE[0].equals(nextEnd)){
                        continue;    
                    }
                    CheckList.put(newPath,1);
                    ArrayList<String> checkTri = NodeAdjList.get(SE[0]);
                    if(checkTri.contains(nextEnd)){
                        numTri++;
                    }
                }

                newEnds = NodeAdjList.get(SE[0]);
                newE = newEnds.iterator();
                while(newE.hasNext()){
                    String nextEnd = newE.next();
                    String newPath = SE[1]+","+SE[0]+","+nextEnd;
                    if(CheckList.containsKey(newPath)|CheckList.containsKey(nextEnd+","+SE[0]+","+SE[1])|SE[1].equals(nextEnd)){
                        continue;    
                    }
                    CheckList.put(newPath,1);
                    ArrayList<String> checkTri = NodeAdjList.get(SE[1]);
                    if(checkTri.contains(nextEnd)){
                        numTri++;
                    }
                }

            }
            String temp = Integer.toString(CheckList.size());
            context.write(new Text("|Category   |Total Number of Triples|Total Number of Triangels|Global Clustering Coefficient"),new Text(""));
            context.write(new Text("|Measurement|"+center(23,Integer.toString(numTri/3))+"|"+center(25,temp)+"|"+String.valueOf((numTri+0.0)/(CheckList.size()+0.0))),new Text(""));
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
        job1.setJarByClass(ClusterCoefficient.class);
        job1.setMapperClass(ClusterCoefficient.DegOneTokenizer.class);
        job1.setReducerClass(ClusterCoefficient.DegOneReducer.class);
        job1.setNumReduceTasks(1);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/One"));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf1, "DegTwo");
        job2.setJarByClass(ClusterCoefficient.class);
        job2.setMapperClass(ClusterCoefficient.AdjTokenizer.class);
        job2.setReducerClass(ClusterCoefficient.DegTwoReducer.class);
        job2.setNumReduceTasks(1);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]+"/One"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/Two"));
        job2.waitForCompletion(true);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
