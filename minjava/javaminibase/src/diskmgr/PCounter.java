package diskmgr;

public class PCounter {
    public static int rCounter = 0;
    public static int wCounter = 0;
//    public static void initialize() {
//        rCounter = 0;
//        wCounter = 0;
//    }
    public static void readIncrement() {
        rCounter++;
    }
    public static void writeIncrement() {
        wCounter++;
    }
}
