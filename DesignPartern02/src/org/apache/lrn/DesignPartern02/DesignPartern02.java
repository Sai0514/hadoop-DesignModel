package org.apache.lrn.DesignPartern02;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DesignPartern02 {
	private static class DataMapper extends Mapper<Object, Text, Text, Text>{	
		
		@Override
		//实现map函数   
		public void map (Object key, Text value, Context context
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
				//long add=Long.parseLong(upload)+Long.parseLong(download);
				
				Text kw=new Text(userid+"\t"+spname); 			
				Text val=new Text(Long.parseLong(upload)+"\t"+Long.parseLong(download)); 
				context.write(kw,val);
			}
		}
	}

	//继承泛型类Reducer
	public static class DataReducer extends Reducer<Text,Text,Text,Text> {

		//实现reduce
		@Override
		public void reduce(Text key, Iterable<Text> values, 
                    Context context
                    ) throws IOException, InterruptedException {
			long up=0;
			long down=0;
			long sum=0;
		    List<DefinedSort> sorts = new ArrayList<DefinedSort>();

			//循环values
			for (Text array : values) {
				String[] ary=array.toString().split("\t");	
				up +=Long.parseLong(ary[0]);
				down +=Long.parseLong(ary[1]);	
	            //System.out.println(sum);
				sum = up+down;	 
				String[] str=key.toString().split("\t");
				DefinedSort ds=new DefinedSort(str[0],str[1],sum);
				sorts.add(ds);
				Collections.sort(sorts);
			 }               
	         
			 for (DefinedSort d : sorts) {
				 context.write(new Text(d.getU()+""),new Text(d.getP()+"\t"+d.getSum()));
			 }		
		}	
	}
 
	public static void main(String[] args) throws Exception{

		//实例化Configuration
		Configuration conf1 = new Configuration();
		//总结上面：返回数组【一组路径】
		String[] otherArgs1 = new GenericOptionsParser(conf1, args).getRemainingArgs();
		//如果只有一个路径，则输出需要有输入路径和输出路径
		if (otherArgs1.length != 2) {
			System.err.println("Usage: datacount <in> <out>");
			System.exit(2);
		}	
		
		Job job1 = new Job(conf1, "data count");
		//为了能够找到DesignPartern02这个类
		job1.setJarByClass(DesignPartern02.class);
		
		//指定map类型
		job1.setMapperClass(DataMapper.class);		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		//指定map类型
		job1.setReducerClass(DataReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);	
		
		//添加输入路径	
		FileInputFormat.addInputPath(job1, new Path(otherArgs1[0]));	
		//添加输出路径
		FileOutputFormat.setOutputPath(job1,new Path(otherArgs1[1]));				
		//提交job
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
				
	}
 
}
