package org.clshen.jdbc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HdfsDemo {

	public static void main(String[] args) throws IOException {
		HdfsDemo oHdfsDemo = new HdfsDemo();
		oHdfsDemo.listFiles();
	}

	public void listFiles() throws IOException {
		System.setProperty("HADOOP_USER_NAME", "cloudera");
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020");
		FileSystem fs = FileSystem.newInstance(conf);
		// true 表示递归查找 false 不进行递归查找
		RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(
				new Path("/user/cloudera"), true);
		while (iterator.hasNext()) {
			LocatedFileStatus next = iterator.next();
			System.out.println(next.getPath());
		}
		System.out
				.println("----------------------------------------------------------");
		FileStatus[] fileStatuses = fs.listStatus(new Path("/user/cloudera"));
		for (int i = 0; i < fileStatuses.length; i++) {
			FileStatus fileStatus = fileStatuses[i];
			System.out.println(fileStatus.getPath());
		}
	}
}
