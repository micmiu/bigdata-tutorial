package com.micmiu.hadoop.hdfs.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.Date;

/**
 * HDFS java客户端的基本操作
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 6/8/2015
 * Time: 13:52
 */
public class HdfsClient {

	private static Configuration conf = null;
	private static FileSystem fs = null;


	public HdfsClient() {
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		HdfsClient client = new HdfsClient();
		String splitStr = "-----------------------------";

		System.out.println(splitStr);
		client.printInfo();

		System.out.println(splitStr);
		client.checkFileExist();

		System.out.println(splitStr);
		String pathuri = "hdfs://edatans/user/root/micmiu/newdir";
		client.mkdir(pathuri);

		System.out.println(splitStr);
		String filepath = "hdfs://edatans/user/root/micmiu/create.txt";
		client.createFile(filepath);

		System.out.println(splitStr);
		client.readFile(filepath);

		System.out.println(splitStr);
		client.getFileBlockLocation(filepath);

		System.out.println(splitStr);
		pathuri = "hdfs://edatans/user/root/micmiu/";
		client.listAllFile(pathuri, true);

		System.out.println(splitStr);
		client.putFileToHDFS("/Users/micmiu/Downloads/mytest.txt", "hdfs://edatans/user/root/micmiu/");

		fs.close();

	}

	public void printInfo() {
		System.out.println(">>>> fs uri    = " + fs.getUri());
		System.out.println(">>>> fs scheme = " + fs.getScheme());
		Path home = fs.getHomeDirectory();
		System.out.println(">>>> home path = " + home.toString());
		listDataNodeInfo();

	}

	public void checkFileExist() throws Exception {
		String pathuri = "hdfs://edatans/user/root/micmiu/demo";
		Path path = new Path(pathuri);
		System.out.println(pathuri + " exist :" + fs.exists(path));
		pathuri = "hdfs://edatans/user/root/micmiu/temp";
		path = new Path(pathuri);
		System.out.println(pathuri + " exist :" + fs.exists(path));
	}

	/**
	 * 读取hdfs指定目录下文件列表
	 *
	 * @param pathuri
	 * @param recursion
	 * @throws Exception
	 */
	public void listAllFile(String pathuri, boolean recursion) throws Exception {
		this.listFile(new Path(pathuri), recursion);
	}


	/**
	 * 读取hdfs指定目录下文件列表
	 *
	 * @param path
	 * @param recursion
	 * @throws Exception
	 */
	private void listFile(Path path, boolean recursion) throws Exception {

		FileStatus[] fileStatusList = fs.listStatus(path);
		for (FileStatus fileStatus : fileStatusList) {
			if (fileStatus.isDirectory()) {
				System.out.println(">>>> dir  : " + fileStatus.getPath());
				if (recursion) {
					listFile(fileStatus.getPath(), recursion);
				}
			} else {
				System.out.println(">>>> file : " + fileStatus.getPath());
			}
		}
	}


	/**
	 * 创建目录
	 *
	 * @param pathuri
	 * @throws Exception
	 */
	public void mkdir(String pathuri) throws Exception {
		Path path = new Path(pathuri);
		if (fs.exists(path)) {
			System.out.println(">>>> " + pathuri + " is exist.");
		} else {
			fs.mkdirs(path);
			System.out.println(">>>> new dir :" + conf.get("fs.default.name") + pathuri);
		}
	}

	/**
	 * 创建hdfs文件
	 */
	public void createFile(String filename) throws Exception {
		FSDataOutputStream os = null;
		BufferedWriter bw = null;
		try {
			Path filePath = new Path(filename);
			System.out.println("Create file : " + filePath.getName() + " to " + filePath.getParent());
			os = fs.create(filePath, true);
			bw = new BufferedWriter(new OutputStreamWriter(os, "utf-8"));
			bw.write("你好 BufferedWrite , Welcome to Hadoop");
			bw.newLine();
			bw.write("Michael'blog : www.micmiu.com.");
			bw.newLine();
			bw.close();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != bw) {
				bw.close();
			}
			if (null != os) {
				os.close();
			}
		}

	}

	/**
	 * 创建一个新的空文件
	 */
	public void createEmptyFile(String filename) throws Exception {
		fs.createNewFile(new Path(filename));
	}

	/**
	 * 创建hdfs文件
	 */
	public void createFile2(String filename) throws Exception {
		FSDataOutputStream os = null;
		Writer out = null;
		try {
			Path filePath = new Path(filename);
			System.out.println("Create file : " + filePath.getName() + " to " + filePath.getParent());

			os = fs.create(filePath, true);
			out = new OutputStreamWriter(os, "utf-8");
			out.write("你好 Write, welcome to Hadoop");
			out.write("\r\n");
			out.write("Michael'blog : www.micmiu.com.");
			out.write("\r\n");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != out) {
				out.close();
			}
			if (null != os) {
				os.close();
			}
		}

	}

	/**
	 * 读取hdfs中的文件内容
	 */
	public void readFile(String pathuri) throws Exception {
		FSDataInputStream is = null;
		BufferedReader br = null;
		try {
			Path filePath = new Path(pathuri);
			is = fs.open(filePath);
			br = new BufferedReader(new InputStreamReader(is, "utf-8"));
			String line;
			while ((line = br.readLine()) != null) {
				System.out.println(">>>> line : " + line);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != br) {
				br.close();
			}
			if (null != is) {
				is.close();
			}
		}
	}

	/**
	 * 取得文件块所在的位置..
	 */
	public void getFileBlockLocation(String pathuri) {
		try {
			Path filePath = new Path(pathuri);
			FileStatus fileStatus = fs.getFileStatus(filePath);
			if (fileStatus.isDirectory()) {
				System.out.println("**** getFileBlockLocations only for file");
				return;
			}
			System.out.println(">>>> file block location:");
			BlockLocation[] blkLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
			for (BlockLocation currentLocation : blkLocations) {
				String[] hosts = currentLocation.getHosts();
				for (String host : hosts) {
					System.out.println(">>>> host: " + host);
				}
			}

			//取得最后修改时间
			long modifyTime = fileStatus.getModificationTime();
			Date d = new Date(modifyTime);
			System.out.println(">>>> ModificationTime = " + d);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 列出所有DataNode的名字信息
	 */
	public void listDataNodeInfo() {
		try {
			DistributedFileSystem hdfs = (DistributedFileSystem) fs;
			DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
			String[] names = new String[dataNodeStats.length];
			System.out.println(">>>> List of all the datanode in the HDFS cluster:");

			for (int i = 0; i < names.length; i++) {
				names[i] = dataNodeStats[i].getHostName();
				System.out.println(">>>> datanode : " + names[i]);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * 读取本地文件上传到HDFS
	 *
	 * @param localFileStr
	 * @param dstFileStr
	 */
	public void putFileToHDFS(String localFileStr, String dstFileStr) {
		putFileToHDFS(true, localFileStr, dstFileStr);
	}

	/**
	 * 手工IO实现把本地文件上传到HDFS
	 *
	 * @param override
	 * @param localFileStr
	 * @param dstFileStr
	 */
	public void putFileToHDFS(Boolean override, String localFileStr, String dstFileStr) {
		FileInputStream is = null;
		BufferedReader br = null;
		FSDataOutputStream os = null;
		BufferedWriter bw = null;
		try {
			File localFile = new File(localFileStr);
			is = new FileInputStream(localFile);
			br = new BufferedReader(new InputStreamReader(is, "utf-8"));
			Path dstTmpPath = new Path(dstFileStr);
			Path dstPath = dstTmpPath;
			if (fs.exists(dstTmpPath)) {
				FileStatus fileStatus = fs.getFileStatus(dstTmpPath);
				if (fileStatus.isDirectory()) {
					dstPath = new Path(dstTmpPath.toString() + "/" + localFile.getName());
				} else if (!override) {
					System.out.println("**** dst file is exist, can't override.");
					return;
				}
			}
			os = fs.create(dstPath, true);
			bw = new BufferedWriter(new OutputStreamWriter(os, "utf-8"));

			String line;
			while ((line = br.readLine()) != null) {
				bw.write(line);
				bw.newLine();
			}
			System.out.println(">>>> put local " + localFile.getName() + " to hdfs " + dstPath.toString() + " success");
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			IOUtils.closeStream(bw);
			IOUtils.closeStream(os);
			IOUtils.closeStream(br);
			IOUtils.closeStream(is);

		}
	}

	/**
	 * 本地文件上传hdfs
	 *
	 * @param override
	 * @param localFileStr
	 * @param dstFileStr
	 */
	public void copyFromLocalFile(Boolean override, String localFileStr, String dstFileStr) {
		try {
			fs.copyFromLocalFile(false, override, new Path(localFileStr), new Path(dstFileStr));
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * 本地文件上传hdfs
	 *
	 * @param localFileStr
	 * @param dstFileStr
	 */
	public void copyFromLocalFile(String localFileStr, String dstFileStr) {
		this.copyFromLocalFile(true, localFileStr, dstFileStr);
	}

	/**
	 * 复制hdfs文件到本地
	 *
	 * @param delSrc
	 * @param localFileStr
	 * @param dstFileStr
	 */
	public void copyToLocalFile(Boolean delSrc, String localFileStr, String dstFileStr) {
		try {
			fs.copyToLocalFile(delSrc, new Path(localFileStr), new Path(dstFileStr));
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * 复制hdfs文件到本地
	 *
	 * @param localFileStr
	 * @param dstFileStr
	 */
	public void copyToLocalFile(String localFileStr, String dstFileStr) {
		copyToLocalFile(false, localFileStr, dstFileStr);
	}

}


