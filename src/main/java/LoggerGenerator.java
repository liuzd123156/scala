import org.apache.log4j.Logger;

public class LoggerGenerator {

//    private static Logger logger = Logger.getLogger(LoggerGenerator.class.getName());

    public static void main(String[] args) throws Exception{
        int index = 1;
        while (true) {
            Thread.sleep(10000);
            Logger logger = Logger.getLogger("logger-"+index%2);
            logger.info("ruoze: " + index++);
        }
    }
}
