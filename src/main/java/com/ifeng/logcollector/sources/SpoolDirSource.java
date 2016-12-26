package com.ifeng.logcollector.sources;

import com.ifeng.configurable.Configurable;
import com.ifeng.configurable.Context;
import com.ifeng.kafka.exceptions.EmptyWorkThreadException;
import com.ifeng.kafka.producer.KafkaProducerEx;
import com.ifeng.kafka.producer.ProducerFactory;
import com.ifeng.logcollector.constances.FILE_POSTFIX;
import com.ifeng.logcollector.sinks.AvroProxy;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Created by zhanglr on 2016/9/28.
 */
public class SpoolDirSource extends AbsDataSource implements Configurable {
    private ExecutorService executorService = Executors.newFixedThreadPool(2);
    private WatchService watchService;
    private String spooldir;

    private BlockingQueue<File> files = new LinkedBlockingQueue<>();
    private String watchFileTypes;
    private Set fileTypeSets = new HashSet();
    private Logger logger = Logger.getLogger(SpoolDirSource.class);

    private static ConcurrentHashMap<WatchKey,Path> pathMap = new ConcurrentHashMap<>();

    public SpoolDirSource() {
        try {
            watchService = FileSystems.getDefault().newWatchService();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    class MonitorDir implements Runnable {
        Pattern pattern = Pattern.compile(FILE_POSTFIX.FILE_COMPLETED);
        public void searchPath(File file){
            try {
                for (File f : file.listFiles()) {
                    if (f.isDirectory()) {
                        Path path = Paths.get(f.getAbsolutePath());
                        WatchKey key = path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
                        pathMap.put(key, path);
                        searchPath(f);
                    }else{
                        String cmp = f.getAbsolutePath().substring(f.getAbsolutePath().lastIndexOf("."),f.getAbsolutePath().length()).toUpperCase();
                        if (!pattern.matcher(f.getAbsolutePath()).find() && fileTypeSets.contains(cmp)){
                            files.add(f);
                        }
                    }
                }
            }catch (IOException e) {
                logger.error(e);
            }
        }

        @Override
        public void run() {
            Path path = Paths.get(spooldir);
            WatchKey key;

            try {
                key = path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
                pathMap.put(key,path);
                File file = new File(spooldir);
                searchPath(file);
                if (files.size() > 0){
                    signalNewFile();
                }
                logger.info("watch dir:"+spooldir);
            } catch (IOException e) {
                logger.error(e);
            }
            do {
                WatchKey wk = null;
                boolean valid = false;
                try {

                    boolean pathChanged = true;
                    while (pathChanged) {
                        wk = watchService.take();
                        for (WatchEvent<?> event : wk.pollEvents()) {
                            final Path changedPath = (Path) event.context();
                            logger.info("file changed " + changedPath.getFileName() + "event.kind() " + event.kind());
                            if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
                                Path p = pathMap.get(wk);
                                if (p == null){
                                    continue;
                                }
                                String pStr = StringUtils.removeEnd(p.toString(), "/") + "/" + changedPath.getFileName();
                                logger.info("get file path "+pStr);
                                File file = new File(pStr);
                                if (file.isDirectory()){
                                    path = Paths.get(file.getAbsolutePath());
                                    key = path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
                                    pathMap.put(key,path);
                                    continue;
                                }

                                String cmp = file.getAbsolutePath().substring(file.getAbsolutePath().lastIndexOf("."),file.getAbsolutePath().length()).toUpperCase();
                                if (pattern.matcher(pStr).find()
                                        ||!fileTypeSets.contains(cmp)){//只针对指定的文件类型解析，目的是防止在copy文件时 程序自动读取临时文件
                                    logger.error("get error file type:"+file.getAbsolutePath());
                                    continue;
                                }
                                logger.info("file added "+changedPath.getFileName());

                                files.put(file);
                                signalNewFile();
                            }
                        }
                        valid = wk.reset();
                        if (!valid) {
                            break;
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                } finally {
                    if (wk != null && !valid) {
                        wk.reset();
                    }
                }
            } while (restart);
        }
    }

    class ReadFile implements Runnable {
        boolean restart = true;
        private void parseFile(File file) throws Exception{
            String fileName = file.getName();
            String cmp = fileName.substring(fileName.lastIndexOf("."),fileName.length()).toLowerCase();
            StringBuilder prefix = new StringBuilder();
            for (String rex : pathExpression) {
                Pattern pattern = Pattern.compile(rex);
                Matcher m = pattern.matcher(file.getPath());
                if (m.find()) {
                    prefix.append(m.group(0)).append(" ");
                }
            }
            InputStreamReader innerReader;

            boolean readFinished = false;
            switch (cmp){
                case ".gz":
                    innerReader = new InputStreamReader(new GZIPInputStream(new FileInputStream(file),1024), Charset.forName("UTF-8"));
                    readFile(innerReader,prefix.toString());
                    readFinished = true;
                    break;
                case ".zip":
                    ZipFile zipFile = new ZipFile(file);
                    Enumeration<ZipEntry> enumeration = (Enumeration<ZipEntry>) zipFile.entries();
                    while (enumeration.hasMoreElements()){
                        ZipEntry zipElement = enumeration.nextElement();
                        innerReader = new InputStreamReader(zipFile.getInputStream(zipElement), Charset.forName("UTF-8"));
                        readFile(innerReader,prefix.toString());
                    }
                    readFinished = true;
                    break;
                case ".bz":
                    innerReader = new InputStreamReader(new BZip2CompressorInputStream(new FileInputStream(file)), Charset.forName("UTF-8"));
                    readFile(innerReader,prefix.toString());
                    readFinished = true;
                    break;
                default:
                    innerReader = new InputStreamReader(new FileInputStream(file), Charset.forName("UTF-8"));
                    readFile(innerReader,prefix.toString());
                    readFinished = true;
                    break;
            }


            if (readFinished) {
                file.renameTo(new File(file.getAbsolutePath() + FILE_POSTFIX.FILE_COMPLETED));
            }
        }

        private void readFile(InputStreamReader inReader,String fileName){

            BufferedReader reader = new BufferedReader(inReader);
            try {
                String line;
                HashMap<String, String> header = new HashMap<>();
                header.put("log_from", topic.toUpperCase());
                if (proxy.getAliveWorkCount() <= 0){
                    logger.error("restart send work thread");
                    proxy.start();
                }
                logger.info("begin read file" + fileName);
                StringBuilder sb = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    if (writeFileName) {
                        line = sb.append(fileName).append(" ").append(line).toString();
                    }
                    proxy.append(line);
                    sb.setLength(0);
                }
            } catch (Exception e){
                logger.error(e);
            }finally {
                try {
                    if (null != inReader) {
                        inReader.close();
                    }
                    if (null != reader) {
                        reader.close();
                    }
                } catch (IOException e) {
                    logger.error("句柄无法关闭。" + fileName + e);
                }
            }
        }
        @Override
        public void run() {
            try {
                initProxy();

                while (restart) {
                    try {
                        File file = files.peek();
                        if (file == null) {
                            waitNewFile();
                        } else {
                            long lastObjectSize = file.length();
                            while (true) {
                                long currentSize = file.length();
                                if (currentSize == lastObjectSize) {
                                    file = files.poll();
                                    logger.info("poll file:"+file.getName());
                                    parseFile(file);
                                    //file.delete();
                                    break;
                                }
                                Thread.sleep(1000);
                            }
                        }
                    }catch (Exception er){
                        logger.error(er);
                    }
                }
            }finally {
                if (proxy != null) proxy.close();
            }
        }
    }

    private void waitNewFile() {
        lock.lock();
        try {
            condition.await(10000,TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("waitNewUrl - interrupted, error {}", e);
        } finally {
            lock.unlock();
        }
    }

    private void signalNewFile() {
        lock.lock();
        try {
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }
    public void start() {
        executorService.execute(new MonitorDir());
        executorService.execute(new ReadFile());
    }

    @Override
    public void config(Context context) {
        super.config(context);
        spooldir = context.getString("path");
        watchFileTypes = context.getString("watchFileTypes",".STA,.LOG,.TXT,.GZ,.ZIP");
        String[] fileTypes = watchFileTypes.split(",");
        fileTypeSets.addAll(Arrays.asList(fileTypes));
    }
}
