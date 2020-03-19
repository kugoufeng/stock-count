package cn.jeremy.hadoop.stockcount.mr;

import cn.jeremy.hadoop.stockcount.mr.bean.JobStartFiled;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import cn.jeremy.hadoop.stockcount.mr.bean.RawStock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public abstract class BaseStockCount {
    private Map<String, String> selfConf = new HashMap<>();

    private String countDate;

    public abstract String outPathTag();

    public void setSelfConf(String key, String value) {
        selfConf.put(key, value);
    }

    public void setCountDate(String value) {
        selfConf.put("count.date", value);
        this.countDate = value;
    }

    public void start(JobStartFiled jobStartFiled)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //设置自定义参数
        if (selfConf.size() > 0) {
            for (Entry<String, String> entry : selfConf.entrySet()) {
                conf.set(entry.getKey(), entry.getValue());
            }
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(jobStartFiled.getJarCls());
        job.setMapperClass(jobStartFiled.getMapperCls());
        job.setReducerClass(jobStartFiled.getReducerCls());
        job.setMapOutputKeyClass(jobStartFiled.getMapperOutKeyCls());
        job.setMapOutputValueClass(jobStartFiled.getMapperOutValueCls());
        job.setOutputKeyClass(jobStartFiled.getOutPutKeyCls());
        job.setOutputValueClass(jobStartFiled.getOutPutValueCls());
        job.setNumReduceTasks(jobStartFiled.getReduceTasksNum());
        FileInputFormat.setInputPaths(job, new Path(jobStartFiled.getInputPath()));
        String outPathStr = jobStartFiled.getOutPath();
        outPathStr = countDate == null ? outPathStr : outPathStr.concat(countDate).concat("/");
        outPathStr = outPathTag() == null ? outPathStr : outPathStr.concat(outPathTag());
        Path outPath = new Path(outPathStr);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

    public static List<RawStock> sortRawStockList(Iterable<RawStock> values, Configuration conf,
                                                  boolean isCompareDate) {
        Iterator<RawStock> iterator = values.iterator();
        List<RawStock> list = new ArrayList<>();
        while (iterator.hasNext()) {
            RawStock next = iterator.next();
            try {
                list.add(next.clone());
            } catch (CloneNotSupportedException e) {
                //
            }
        }
        Collections.sort(list);
        if (isCompareDate) {
            String countDate = conf.get("count.date");
            if (null != countDate) {
                Date date = timeStr2Date(countDate);
                if (date.compareTo(list.get(0).getToday()) == 0) {
                    return list;
                }
            }
            return null;
        } else {
            return list;
        }
    }

    public static Date timeStr2Date(String time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", new Locale("EN"));
        if (null == time) {
            throw new IllegalArgumentException("time parameter can not be null");
        }
        try {
            return sdf.parse(time);
        } catch (ParseException e) {
            throw new IllegalArgumentException("using [yyyy-MM-dd] parse [" + time + "] failed");
        }
    }

}
