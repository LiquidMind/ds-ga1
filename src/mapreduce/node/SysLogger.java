package mapreduce.node;

import java.io.IOException;
import java.util.logging.*;

/**
 * Created by Aidar on 16.06.2015.
 */
public class SysLogger {
    private Logger _logger;
    private FileHandler fh;
    private ConsoleHandler ch;
    private static SysLogger _instance;

    private SysLogger(){
        _logger=Logger.getLogger("SysLog");
    }

    /**
     * Getting an instance of singleton
     * @return
     */
    public static synchronized SysLogger getInstance(){
        if (_instance==null){
            _instance=new SysLogger();
        }
        return _instance;
    }

    /**
     * setup filename
     * @param filename
     */
    public void setup (String filename){
        //the code chunk was particularly taken from
        //http://stackoverflow.com/questions/15758685/how-to-write-logs-in-text-file-when-using-java-util-logging-logger
        try {
            _logger.setUseParentHandlers(false);
            // This block configure the logger with handler and formatter
            fh = new FileHandler(filename);
            _logger.addHandler(fh);
            Formatter formatter=new Formatter() {
                public String format(LogRecord record) {
                    return "["+record.getMillis() + "] " + record.getLevel() + ": "
                            + record.getSourceClassName() + "-:-"
//                            + record.getSourceMethodName() + "-:-"
                            + record.getMessage() + "\n";
                }
            };
            fh.setFormatter(formatter);

            ch = new ConsoleHandler();
            ch.setFormatter(formatter);
            _logger.addHandler(ch);

            _logger.info("Logger is ready to go with file " + filename);

        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void info(String msg){
        _logger.info(msg);
    }

    public void warning(String msg){
        _logger.warning(msg);
    }
}
