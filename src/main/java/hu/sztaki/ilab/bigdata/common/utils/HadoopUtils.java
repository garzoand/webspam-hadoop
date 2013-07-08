package hu.sztaki.ilab.bigdata.common.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.log4j.Logger;

/**
 * Utility class for miscellaneous helper tasks
 * 
 * @author Bendig Loránd <lbendig@ilab.sztaki.hu>
 *
 */
public class HadoopUtils {

    private static final Logger LOG = Logger.getLogger(HadoopUtils.class);
    
    private HadoopUtils() {
        throw new AssertionError("shouldn't be instantiated");
    }

    /**
     * Sets class instance in the Configuration object 
     * as a serialized String value 
     * 
     * @param key - Configuration parameter key
     * @param value - Serializable object instance 
     * @param conf - Configuration object
     */
    public static void setObject(String key, Serializable value, Configuration conf) {
        
        if (StringUtils.isEmpty(key) || value == null) {
            return;
        }
        
        ObjectOutputStream oos = null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(value);
            conf.set(key, Base64.encodeBytes(baos.toByteArray()));
        }
        catch (Exception e) {
            String msg = "Can't serialize configuration value!";
            LOG.error(msg, e);
            throw new RuntimeException(msg, e);
        }
        
        finally {
            IOUtils.closeQuietly(oos);
        }
    }
    
    /**
     * Returns the class instance from the Configuration object by deserializing the 
     * corresponding value
     * 
     * @param <T> - Class instance type
     * @param key - Configuration parameter key
     * @param clazz - type token
     * @param conf - Configuration object
     * @return - T retrieved from the configuration
     */
    public static <T extends Serializable> T getObject(String key, Class<T> clazz,
            Configuration conf)  {

        T result = null;

        if (StringUtils.isEmpty(key)) {
            return null;
        }

        String strValue = conf.get(key);
        if (StringUtils.isEmpty(strValue)) {
            return null;
        }

        ObjectInputStream ois = null;
        try {

            byte[] data = Base64.decode(strValue);
            ois = new ObjectInputStream(new ByteArrayInputStream(data));
            result = clazz.cast(ois.readObject());
        } 
        catch (Exception e) {
            String msg = "Can't deserialize configuration value!";
            LOG.error(msg, e);
            throw new RuntimeException(msg, e);
        }
        finally {
            IOUtils.closeQuietly(ois);
        }
        return result;
    }
    
    /**
     * Makes the given jars available to the running tasks by adding them to
     * the DistributedCache. Resources are taken from the command line argument
     * value of '-hdfslibs' which can be

     * @param localtion - can be a path to a single jar or to a directory
     * @param conf
     * @throws IOException
     * @throws URISyntaxException
     */
    public static void addDependenciesToDistributedCache(String location, Configuration conf)
            throws IOException, URISyntaxException {

        if (location == null || conf == null) {
            throw new IllegalArgumentException("Invalid arguments");
        }

        final String filterContents = "/*.jar";
        final String filterContent = ".jar";

        FileSystem fs = FileSystem.get(conf); 
        FileStatus pathFileStatus = fs.getFileStatus(new Path(location));

        // can be a directory or a single jar
        if (pathFileStatus.isDir()) {
            FileStatus[] listStatus = fs.globStatus(new Path(location + filterContents));
            for (FileStatus fstat : listStatus) {
                if (!fstat.isDir()) {
                    Path p = new Path(fstat.getPath().toUri().getPath());
                    DistributedCache.addFileToClassPath(p, conf);
                }
            }
        }
        else if (location.endsWith(filterContent)) {
            DistributedCache.addFileToClassPath(pathFileStatus.getPath(), conf);
        }
    }
    
    /**
     * Returns the value of the hdfslibs command line option
     *  
     * @param args Command line argument of 'hdfslibs'
     * @return value of the argument
     */
    public static String parseDistributedCacheLibsOption(String[] args) {

        String result = null;
        //however, command line args cannot be null..
        if (args == null || args.length == 0) {
            return result;
        }

        for (String arg : args) {
            if (arg.startsWith("-hdfslibs")) {
                return result = arg.split("=")[1];
            }
        }
        return result;
    }
    
    /**
     * Returns all files' Path in all subdirectories within a given directory 
     * 
     * @param fs - current FileSystem
     * @param basePath - input directory
     * @return an array of Path objects that contains all files within basePath
     * @throws IOException
     * @throws URISyntaxException
     */
    public static Path[] getRecursivePaths(FileSystem fs, String basePath) throws IOException, URISyntaxException {
        List<Path> result = new ArrayList<Path>();
        basePath = fs.getUri() + basePath;
        FileStatus[] listStatus = fs.globStatus(new Path(basePath+"/*"));
        for (FileStatus fstat : listStatus) {
            readSubDirectory(fstat, basePath, fs, result);
        }
        return (Path[]) result.toArray(new Path[result.size()]);
        
    }
    
    private static void readSubDirectory(FileStatus fileStatus, String basePath,
            FileSystem fs, List<Path> paths) throws IOException, URISyntaxException {
        
        if (!fileStatus.isDir()) {
            paths.add(fileStatus.getPath());
        }
        else {
            String subPath = fileStatus.getPath().toString();
            FileStatus[] listStatus = fs.globStatus(new Path(subPath + "/*"));
            if (listStatus.length == 0) {
                paths.add(fileStatus.getPath());
            }
            for (FileStatus fst : listStatus) {
                readSubDirectory(fst, subPath, fs, paths);
            }
        }
    }
    
}
