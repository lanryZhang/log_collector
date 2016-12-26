package com.ifeng.logcollector.sources;

import com.ifeng.configurable.Configurable;
import com.ifeng.configurable.Context;
import com.ifeng.kafka.producer.KafkaProducerEx;
import com.ifeng.kafka.producer.ProducerFactory;
import com.ifeng.logcollector.sinks.AvroProxy;
import com.ifeng.logcollector.sinks.IProxy;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhanglr on 2016/9/23.
 */
public class ExecSource extends AbsDataSource implements Runnable,Configurable {
    public static final  Logger logger = Logger.getLogger(ExecSource.class);
    private String command = "";
    private boolean restart = true;
    private AtomicInteger cnt = new AtomicInteger(0);
    private IProxy proxy = null;
    private Process process = null;
    protected boolean logStderr;

    public ExecSource(){}

    @Override
    public void run() {
        do {
            BufferedReader reader = null;
            try {
                String[] commandArgs = command.split("\\s+");

                ProcessBuilder processBuilder = new ProcessBuilder(commandArgs);
                process = processBuilder.start();

                initProxy();
                //proxy = new AvroProxy(hostIp,port,batchSize,topic,batchTimeout,false);
                String line;
                HashMap<String, String> header = new HashMap<>();
                header.put("log_from", topic.toUpperCase());
                reader = new BufferedReader(new InputStreamReader(process.getInputStream(), Charset.forName("UTF-8")));

                StderrReader stderrReader = new StderrReader(new BufferedReader(
                        new InputStreamReader(process.getErrorStream(), charset)), logStderr);
                stderrReader.setName("StderrReader-[" + command + "]");
                stderrReader.setDaemon(true);
                stderrReader.start();

                while ((line = reader.readLine()) != null) {
                    logger.info(line);
                    if (null != localIp){
                        line = localIp + " " + line;
                    }
                    proxy.append(line);
                }
                proxy.flush();

            } catch (IOException e) {
                logger.error(e);
            } catch (Exception er) {
                logger.error(er);
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        logger.error(e);
                    }
                }
                if (process != null) {
                    synchronized (process) {
                        process.destroy();
                        String exitCode = String.valueOf(kill());
                        if (restart) {
                            try {
                                logger.info("exit code " + exitCode);
                                Thread.sleep(2 * 1000);
                            } catch (InterruptedException e) {
                                logger.error(e);
                            }
                            logger.error("reconnect " + cnt.getAndIncrement());
                        }
                    }
                }
            }
        }while (restart);
    }

    @Override
    public void config(Context context) {
        super.config(context);
        command = context.getString("command");
        charset = context.getString("charset");
        batchTimeout = context.getLong("batchTimeout", 5000L);

    }

    static class EventBuilder{
        static AvroFlumeEvent wrap(Map header, ByteBuffer body){
            return new AvroFlumeEvent(header,body);
        }
        static AvroFlumeEvent wrap(ByteBuffer body){
            return new AvroFlumeEvent(null,body);
        }
    }


    private static class StderrReader extends Thread {
        private BufferedReader input;
        private boolean logStderr;

        protected StderrReader(BufferedReader input, boolean logStderr) {
            this.input = input;
            this.logStderr = logStderr;
        }

        @Override
        public void run() {
            try {
                int i = 0;
                String line = null;
                while((line = input.readLine()) != null) {
                    if(logStderr) {
                        // There is no need to read 'line' with a charset
                        // as we do not to propagate it.
                        // It is in UTF-16 and would be printed in UTF-8 format.
                        logger.info("StderrLogger "+ ++i + line);
                    }
                }
            } catch (IOException e) {
                logger.info("StderrLogger exiting", e);
            } finally {
                try {
                    if(input != null) {
                        input.close();
                    }
                } catch (IOException ex) {
                    logger.error("Failed to close stderr reader for exec source", ex);
                }
            }
        }
    }

    public int kill() {
        if(process != null) {
            synchronized (process) {
                process.destroy();

                try {
                    int exitValue = process.waitFor();
                    return exitValue;
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
            return Integer.MIN_VALUE;
        }
        return Integer.MIN_VALUE / 2;
    }

    boolean timeout(){
        if ((System.currentTimeMillis() - lastFlushTime) > batchTimeout){
            return true;
        }
        return false;
    }
}
