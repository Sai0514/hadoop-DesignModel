//过滤:实现将部分的server访问记录存放在不同的文件

package org.apache.lrn.DesignPartern01;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class DesignPartern01 {
	public static class DataMapper extends Mapper<LongWritable, Text, Text, Text>{	
		//public Text word=new Text();
		//实现map函数   
		public void map (LongWritable key, Text value, Context context
                 ) throws IOException, InterruptedException {	
			//java分隔符获得字符串
			//StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			String line=value.toString();  	//将输入文本转化为字符串类型
			String[] itr=line.split("\n");	//按照空格符分割字符串
			for(int i=0;i<itr.length;i++){  
				String[] str=itr[i].split("\t");  // 对每行字符串进行分割，分隔符是制表符“\t”
				
				if(str.length!=7){
					System.out.println("Error: the unreasonable data!");
					return ;
				}
				
				String userid=str[1];  //user
				String spname=str[4];  //server
				String upload=str[5];  //upload
				String download=str[6];//download
				String one="1"; //counts
						
				Text kw=new Text(userid+"\t"+spname);    //自定义Text数据类型key
				Text val=new Text(one+"\t"+upload+"\t"+download); //自定义Text数据类型value
				
				context.write(kw,val);	
			}
		}
	}
	
	
	//继承泛型类Reducer
	public static class DataReducer extends Reducer<Text,Text,Text,Text> {
		//创建MultipleOutputs对象 
		 private MultipleOutputs<Text, Text> multioutput;    
		 protected void setup(Context context) throws IOException,InterruptedException {  
		    	multioutput = new MultipleOutputs<Text, Text>(context);  
		 }  
		 protected void cleanup(Context context) throws IOException,InterruptedException {  
		    	multioutput.close();  
		 }  
		
		 //实现reduce
		public void reduce(Text key, Iterable<Text> values, 
                    Context context
                    ) throws IOException, InterruptedException {
			long up=0;
			long down=0;
			int sum=0;
			//循环values
			for (Text array : values) {
				String[] ary=array.toString().split("\t");
				sum+=Integer.parseInt(ary[0]);
				up+= Long.parseLong(ary[1]) ;
				down+= Long.parseLong(ary[2]) ;
			}
			
			String iterm=key.toString();		
			String[] itr=iterm.split("\t");				
			//System.out.println(itr[1]);
			
				if (itr[1].equals("360")){
					multioutput.write(itr[1],key,new Text(sum+"\t"+up+"\t"+down));						
				};	
				if (itr[1].equals("google")){
					multioutput.write(itr[1],key,new Text(sum+"\t"+up+"\t"+down));					
				};
				if (itr[1].equals("qq")){
					multioutput.write(itr[1],key,new Text(sum+"\t"+up+"\t"+down));					
				};	
				if (itr[1].equals("baidu")){
					multioutput.write(itr[1],key,new Text(sum+"\t"+up+"\t"+down));						
				};	
				if (itr[1].equals("sina")){
					multioutput.write(itr[1],key,new Text(sum+"\t"+up+"\t"+down));					
				};
				if (itr[1].equals("apple")){
					multioutput.write(itr[1],key,new Text(sum+"\t"+up+"\t"+down));					
				};
					
		}
	}
	
	public static void main(String[] args) throws Exception{

		//实例化Configuration
		Configuration conf = new Configuration();
		//总结上面：返回数组【一组路径】
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//如果只有一个路径，则输出需要有输入路径和输出路径
		if (otherArgs.length != 2) {
			System.err.println("Usage: datacount <in> <out>");
			System.exit(2);
		}
		
		//实例化job

		Job job = new Job(conf, "data count");
		//为了能够找到WordCount这个类
		job.setJarByClass(DesignPartern01.class);
		//指定map类型
		job.setMapperClass(DataMapper.class);					
		job.setReducerClass(DataReducer.class);
		
		//reduce输出Key的类型，是Text
		job.setOutputKeyClass(Text.class);
		//reduce输出Value的类型
		job.setOutputValueClass(Text.class);

		//设置多输出文件
		MultipleOutputs.addNamedOutput(job,"360",TextOutputFormat.class,Text.class,Text.class);  
        MultipleOutputs.addNamedOutput(job,"google",TextOutputFormat.class,Text.class,Text.class);  
        MultipleOutputs.addNamedOutput(job,"qq",TextOutputFormat.class,Text.class,Text.class);
		MultipleOutputs.addNamedOutput(job,"baidu",TextOutputFormat.class,Text.class,Text.class);  
        MultipleOutputs.addNamedOutput(job,"sina",TextOutputFormat.class,Text.class,Text.class);  
        MultipleOutputs.addNamedOutput(job,"apple",TextOutputFormat.class,Text.class,Text.class);
		
		//添加输入路径	
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));	
		//添加输出路径
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));		
		
		//提交job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	
	}

