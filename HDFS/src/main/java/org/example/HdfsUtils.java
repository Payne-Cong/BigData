package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsUtils {

    private static final String HDFS_PATH = "hdfs://127.0.0.1:19000"; // 默认为9000端口，此为 namenode 界面显示的Active的地址
    private static final String HDFS_USER = "";

    public static FileSystem fileSystem = getHDFSFileSystem();;


    public static FileSystem getHDFSFileSystem(){
        FileSystem fileSystem = null;
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", HDFS_PATH);
        try {
            fileSystem = FileSystem.get(URI.create(HDFS_PATH),configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return fileSystem;
    }

    public static void upFile(String srcPath,String dstPath) throws IOException {
        Path src=new Path(srcPath); // 本地要上传的文件
        Path dst=new Path(dstPath); // 放在hdfs上的地方
        FileSystem fileSystem = getHDFSFileSystem();
        fileSystem.copyFromLocalFile(src, dst);
    }

    public static void deleteFile(String path) throws IOException {
        HdfsUtils.fileSystem.delete(new Path(path), true);
    }


    public static void makeDirAndDeleteDir() throws IOException {

        FileSystem fileSystem = getHDFSFileSystem();
        // 创建目录
        fileSystem.mkdirs(new Path("/A"));
        fileSystem.mkdirs(new Path("/A/A1"));
        fileSystem.mkdirs(new Path("/A/A2"));
        fileSystem.mkdirs(new Path("/C"));
        fileSystem.mkdirs(new Path("/D/D1/D2"));

        // 重命名文件或文件夹
        fileSystem.rename(new Path("/A"), new Path("/B"));

        // 删除文件夹 ，如果是非空文件夹，参数2必须给值true
//        fileSystem.delete(new Path("/B"), true);


    }

    public static void findListFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
        FileSystem fileSystem = getHDFSFileSystem();
        // 思考：为什么返回迭代器，而不是List之类的容器
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocations) {
                System.out.println("block-length:" + bl.getLength() + "--" + "block-offset:" + bl.getOffset());
                String[] hosts = bl.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("----打印的分割线------");
        }
    }


    public static void findListAll() throws FileNotFoundException, IllegalArgumentException, IOException {

        FileStatus[] listStatus = HdfsUtils.fileSystem.listStatus(new Path("/"));
        String flag = "d--             ";
        for (FileStatus fstatus : listStatus) {
            if (fstatus.isFile())  flag = "f--     ";
            System.out.println(flag + fstatus.getPath().getName());
        }
    }


    public static void main(String[] args) throws IOException {

//        upFile("D:/dept.txt","/");
        deleteFile("/dept.txt");

//        FileSystem fileSystem = getHDFSFileSystem();
//        FileStatus stats[]=fileSystem.listStatus(new Path("/"));
//        for(int i=0;i<stats.length;i++){
//            System.out.println(stats[i].getPath().toString());
//        }


    }

}
