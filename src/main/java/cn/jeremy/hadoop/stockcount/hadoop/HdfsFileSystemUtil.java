package cn.jeremy.hadoop.stockcount.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import javax.annotation.PostConstruct;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * hadoop文件系统工具
 *
 * @author fengjiangtao
 * @date 2020/3/12 16:08
 */
@Component
public class HdfsFileSystemUtil
{
    @Value("${hadoop.name-node}")
    private String nameNodeUrl;

    @Value("${hadoop.username}")
    private String userName;

    private FileSystem fs;

    @PostConstruct
    private void init()
        throws URISyntaxException, IOException, InterruptedException
    {
        Configuration conf = new Configuration();
        fs = FileSystem.get(new URI(nameNodeUrl), conf, userName);
    }

    /**
     * 删除文件夹及文件
     *
     * @param pathStr
     * @author fengjiangtao
     */
    public void delDir(String pathStr)
        throws IOException
    {
        fs.delete(new Path(pathStr), true);
    }

    /**
     * 判断文件或者文件夹是否存在
     *
     * @param pathStr
     * @return boolean
     * @author fengjiangtao
     */
    public boolean isExist(String pathStr)
        throws IOException
    {
        return fs.exists(new Path(pathStr));
    }

    /**
     * 创建文件夹
     *
     * @param pathStr
     * @author fengjiangtao
     */
    public void createDir(String pathStr)
        throws IOException
    {
        fs.mkdirs(new Path(pathStr));
    }

    /**
     * 上传文件到hdfs系统
     *
     * @param src
     * @param dst
     * @author fengjiangtao
     */
    public void uploadFile(String src, String dst)
        throws IOException
    {
        fs.copyFromLocalFile(new Path(src), new Path(dst));
    }

    /**
     * 下载文件到本地
     *
     * @param src
     * @param dst
     * @author fengjiangtao
     */
    public void downloadFile(String src, String dst)
        throws IOException
    {
        fs.copyToLocalFile(true, new Path(src), new Path(dst));
    }
}
