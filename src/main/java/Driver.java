import job1.*;
import job3.*;
import job4.*;
import job5.*;
import job6.*;
import job7.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.security.sasl.SaslServer;
import java.net.URI;

public class Driver {



    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

//        /*******Driver code of Job1 Question 1 & 2********/
//
//        Job job1 = Job.getInstance(conf, "CS455-PA3");
//        job1.setJarByClass(Driver.class);
//        job1.setNumReduceTasks(1);
//        job1.setMapperClass(Job1Mapper.class);
//        //job1.setSortComparatorClass(Job1Comparator.class);
//        //job1.setCombinerClass(Job1Reducer.class);
//        job1.setReducerClass(Job1Reducer.class);
//        //job1.setMapOutputKeyClass(IntWritable.class);
//        //job1.setMapOutputValueClass(IntWritable.class);
//        job1.setOutputKeyClass(Text.class);
//        job1.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job1, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job1, new Path("./job12_tmp"));
//      job1.waitForCompletion(true);
//
//        Job job1_1 = Job.getInstance(conf, "CS455-PA3");
//        job1_1.setJarByClass(Driver.class);
//        //job1_1.setNumReduceTasks(1);
//        job1_1.setMapperClass(Job1Mapper2.class);
//        //job1.setSortComparatorClass(Job1Comparator.class);
//        //job1_1.setCombinerClass(Job1Reducer2.class);
//        job1_1.setReducerClass(Job1Reducer2.class);
//        //job1.setMapOutputKeyClass(IntWritable.class);
//        //job1.setMapOutputValueClass(IntWritable.class);
//        job1_1.setOutputKeyClass(Text.class);
//        job1_1.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job1_1, new Path("./job12_tmp"));
//        FileOutputFormat.setOutputPath(job1_1, new Path("./Job12"));
//        job1_1.waitForCompletion(true);



        /*******Driver code of Job3********/

//         Job job3 = Job.getInstance(conf, "CS455-PA3");
//        job3.setJarByClass(Driver.class);
//         //job5.setNumReduceTasks(1);
//        job3.setMapperClass(Job3Mapper1.class);
//         //job1.setSortComparatorClass(Job1Comparator.class);
//         //job5.setCombinerClass(Job5Reducer1.class);
//        job3.setReducerClass(Job3Reducer1.class);
//         //job4.setMapOutputKeyClass(IntWritable.class);
//         //job4.setMapOutputValueClass(Text.class);
//        job3.setOutputKeyClass(Text.class);
//        job3.setOutputValueClass(Text.class);
//         FileInputFormat.addInputPath(job3, new Path(args[0]));
//         FileOutputFormat.setOutputPath(job3, new Path("./job3_tmp"));
//        job3.waitForCompletion(true);
//
////         DistributedCache.addCacheFile(new URI("main/data/supplementary/airports.csv"), conf);
//         DistributedCache.addCacheFile(new URI("./airports.csv"), conf);
//         Job job3_1 = Job.getInstance(conf, "CS455-PA3");
//        job3_1.setJarByClass(Driver.class);
//         //job6.setNumReduceTasks(1);
//        job3_1.setMapperClass(Job3Mapper2.class);
//         //job7.setSortComparatorClass(Job7Comparator.class);
//         //job6.setCombinerClass(Job4Reducer1.class);
//        job3_1.setReducerClass(Job3Reducer2.class);
//
//        job3_1.setOutputKeyClass(Text.class);
//        job3_1.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job3_1, new Path("./job3_tmp"));
//        FileOutputFormat.setOutputPath(job3_1, new Path("./Job3"));
//        System.exit(job3_1.waitForCompletion(true)?0:1);



        /*******Driver code of Job4********/
//
//        Job job4 = Job.getInstance(conf, "CS455-PA3");
//        job4.setJarByClass(Driver.class);
//        //job5.setNumReduceTasks(1);
//        job4.setMapperClass(Job4Mapper1.class);
//        //job1.setSortComparatorClass(Job1Comparator.class);
//        //job5.setCombinerClass(Job5Reducer1.class);
//        job4.setReducerClass(Job4Reducer1.class);
//        //job4.setMapOutputKeyClass(IntWritable.class);
//        //job4.setMapOutputValueClass(Text.class);
//        job4.setOutputKeyClass(Text.class);
//        job4.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job4, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job4, new Path("./job4_tmp"));
//        job4.waitForCompletion(true);
//
////        DistributedCache.addCacheFile(new URI("main/data/supplementary/carriers.csv"), conf);
//        DistributedCache.addCacheFile(new URI("./carriers.csv"), conf);
//        Job job4_1 = Job.getInstance(conf, "CS455-PA3");
//        job4_1.setJarByClass(Driver.class);
//        //job6.setNumReduceTasks(1);
//        job4_1.setMapperClass(Job4Mapper2.class);
//        //job7.setSortComparatorClass(Job7Comparator.class);
//        //job6.setCombinerClass(Job4Reducer1.class);
//        job4_1.setReducerClass(Job4Reducer2.class);
//
//        job4_1.setOutputKeyClass(Text.class);
//        job4_1.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job4_1, new Path("./job4_tmp"));
//        FileOutputFormat.setOutputPath(job4_1, new Path("./Job4"));
//       System.exit(job4_1.waitForCompletion(true)?0:1);

//
//        /*******Driver code of Job5********/
//
////        DistributedCache.addCacheFile(new URI("main/data/supplementary/plane-data.csv"), conf);
//        DistributedCache.addCacheFile(new URI("./plane-data.csv"), conf);
//        Job job5 = Job.getInstance(conf, "CS455-PA3");
//        job5.setJarByClass(Driver.class);
//        //job5.setNumReduceTasks(1);
//        job5.setMapperClass(Job5Mapper1.class);
//        //job1.setSortComparatorClass(Job1Comparator.class);
//        //job5.setCombinerClass(Job5Reducer1.class);
//        job5.setReducerClass(Job5Reducer1.class);
//        job5.setMapOutputKeyClass(IntWritable.class);
//        job5.setMapOutputValueClass(Text.class);
//        job5.setOutputKeyClass(Text.class);
//        job5.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job5, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job5, new Path("./job5_tmp"));
//        job5.waitForCompletion(true);
//
//        Job job5_1 = Job.getInstance(conf, "CS455-PA3");
//        job5_1.setJarByClass(Driver.class);
//        //job6.setNumReduceTasks(1);
//        job5_1.setMapperClass(Job5Mapper2.class);
//        //job7.setSortComparatorClass(Job7Comparator.class);
//        //job6.setCombinerClass(Job6Reducer1.class);
//        job5_1.setReducerClass(Job5Reducer2.class);
//
//        job5_1.setOutputKeyClass(Text.class);
//        job5_1.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job5_1, new Path("./job5_tmp"));
//        FileOutputFormat.setOutputPath(job5_1, new Path("./Job5"));
//        System.exit(job5_1.waitForCompletion(true)?0:1);

//
//        /*******Driver code of Job6********///(Weather-related delay for each city.)
//        Job job6 = Job.getInstance(conf, "CS455-PA3");
//        job6.setJarByClass(Driver.class);
//        //job6.setNumReduceTasks(1);
//        job6.setMapperClass(Job6Mapper1.class);
//        //job1.setSortComparatorClass(Job1Comparator.class);
//        //job6.setCombinerClass(Job6Reducer1.class);
//        job6.setReducerClass(Job6Reducer1.class);
//        //job6.setMapOutputKeyClass(Text.class);
//        //job6.setMapOutputValueClass(IntWritable.class);
//        job6.setOutputKeyClass(Text.class);
//        job6.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job6, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job6, new Path("./job6_tmp"));
//        job6.waitForCompletion(true);
//
//        //Job6_1
//        //Hard-coded Cache file: hdfs://richmond:PORT_NUM/main/data/supplementary/airports.csv
////        String path = "/home/bbkstha/airports.csv";
////        Path filePath = new Path(path);
////        String uriWithLink = filePath.toUri().toString();
//
////        DistributedCache.addCacheFile(new URI("main/data/supplementary/airports.csv"), conf);
////        DistributedCache.addCacheFile(new URI("main/data/supplementary/plane-data.csv"), conf);
//        DistributedCache.addCacheFile(new URI("./airports.csv"), conf);
//        DistributedCache.addCacheFile(new URI("./plane-data.csv"), conf);
//
//        Job job6_1 = Job.getInstance(conf, "CS455-PA3");
//        job6_1.setJarByClass(Driver.class);
//        //job6.setNumReduceTasks(1);
//        job6_1.setMapperClass(Job6Mapper2.class);
//        //job7.setSortComparatorClass(Job7Comparator.class);
//        //job6.setCombinerClass(Job6Reducer1.class);
//        job6_1.setReducerClass(Job6Reducer2.class);
//
//        job6_1.setOutputKeyClass(Text.class);
//        job6_1.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job6_1, new Path("./job6_tmp"));
//        FileOutputFormat.setOutputPath(job6_1, new Path("./Job6"));
//        job6_1.waitForCompletion(true);

//
//        /*******Driver code of Job7********///(Weather-related delay for each city.)
        Job job7 = Job.getInstance(conf, "CS455-PA3");
        job7.setJarByClass(Driver.class);
        //job6.setNumReduceTasks(1);
        job7.setMapperClass(Job7Mapper1.class);
        //job1.setSortComparatorClass(Job1Comparator.class);
        //job6.setCombinerClass(Job6Reducer1.class);
        job7.setReducerClass(Job7Reducer1.class);
        //job6.setMapOutputKeyClass(Text.class);
        //job6.setMapOutputValueClass(IntWritable.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job7, new Path(args[0]));
        FileOutputFormat.setOutputPath(job7, new Path("./job7_tmp"));
        job7.waitForCompletion(true);

        //Job6_1
        //Hard-coded Cache file: hdfs://richmond:PORT_NUM/main/data/supplementary/airports.csv
//        String path = "/home/bbkstha/airports.csv";
//        Path filePath = new Path(path);
//        String uriWithLink = filePath.toUri().toString();

//        DistributedCache.addCacheFile(new URI("main/data/supplementary/airports.csv"), conf);
//        DistributedCache.addCacheFile(new URI("main/data/supplementary/plane-data.csv"), conf);
//        DistributedCache.addCacheFile(new URI("main/data/supplementary/carriers.csv"), conf);
     DistributedCache.addCacheFile(new URI("./airports.csv"), conf);
     DistributedCache.addCacheFile(new URI("./plane-data.csv"), conf);
     DistributedCache.addCacheFile(new URI("./carriers.csv"), conf);
        Job job7_1 = Job.getInstance(conf, "CS455-PA3");
        job7_1.setJarByClass(Driver.class);
        //job6.setNumReduceTasks(1);
        job7_1.setMapperClass(Job7Mapper2.class);
        //job7.setSortComparatorClass(Job7Comparator.class);
        //job6.setCombinerClass(Job6Reducer1.class);
        job7_1.setReducerClass(Job7Reducer2.class);

        job7_1.setOutputKeyClass(Text.class);
        job7_1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job7_1, new Path("./job7_tmp"));
        FileOutputFormat.setOutputPath(job7_1, new Path("./Job7"));
        System.exit(job7_1.waitForCompletion(true)? 0: 1);

    }
}