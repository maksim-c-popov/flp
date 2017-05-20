import java.util.Random;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class dealGenerator {
   public static Configuration conf = new Configuration(); 
   public static Random rand = new Random(System.currentTimeMillis());

   public static class CreatorMapper extends Mapper<Object, Text, Text, IntWritable>{ 
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException { 

			String innAll = value.toString();
			String[] innDictionary = innAll.split(" ");

			for (int j = 0; j < 500; ++j){
				String result = "";
				
				for (int i = 0; i < 1000; ++i){

					String declNumFirst = String.valueOf(rand.nextInt(1000000-100000)+100000);
					String declNumSecond = String.valueOf(rand.nextInt(1000000-100000)+100000);
					
					String firstINN = innDictionary[rand.nextInt(innDictionary.length)];
					String secondINN = innDictionary[rand.nextInt(innDictionary.length)];
					while (firstINN == secondINN){
						secondINN = innDictionary[rand.nextInt(innDictionary.length)];
					}
					
					String sum = String.valueOf(rand.nextInt(10000000-10000)+10000);
					String date = String.valueOf(rand.nextInt(30)+1)+"."+String.valueOf(rand.nextInt(12)+1)+".2016";
					
					String newDeal="<declaration number='"+declNumFirst+"' inn='"+firstINN+"'>" +
			            	"<invoices>" + "<invoice date='"+date+"'>" + 
			            	"<bargainer inn='"+firstINN+"'>" + "<summ>"+sum+"</summ>" + "</bargainer>" +
			            	"<bargainer inn='"+secondINN+"'>" + "<summ>"+sum+"</summ>" + "</bargainer>" + 
			            	"</invoice>" + "</invoices>" + "</declaration>\n";
					
					if (rand.nextInt(100)+1 > 90){
						firstINN = "0000000000";
					}
					
					newDeal+="<declaration number='"+declNumSecond+"' inn='"+secondINN+"'>" +
			            	"<invoices>" + "<invoice date='"+date+"'>" + 
			            	"<bargainer inn='"+secondINN+"'>" + "<summ>"+sum+"</summ>" + "</bargainer>" +
			            	"<bargainer inn='"+firstINN+"'>" + "<summ>"+sum+"</summ>" + "</bargainer>" + 
			            	"</invoice>" + "</invoices>" + "</declaration>";
			        if (i != 999){    	
			        newDeal+="\n";
					}
					result += newDeal;
				}
				context.write(new Text(result), new IntWritable(1));
			}
		} 
    } 
 
    public static class CreatorReducer extends Reducer<Text,IntWritable,Text,IntWritable> { 
	        private IntWritable result = new IntWritable(); 
	        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			result.set(1);	
			context.write(key, null);
		}
    }
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(); 
        Job job = Job.getInstance(conf, "files creator"); 
        job.setJarByClass(dealGenerator.class); 
        job.setMapperClass(CreatorMapper.class); 
        job.setReducerClass(CreatorReducer.class); 
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
}