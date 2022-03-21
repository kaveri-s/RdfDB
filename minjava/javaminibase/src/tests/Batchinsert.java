package tests;

import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.PCounter;
import global.*;

import java.io.*;
import java.util.Scanner;

import static java.lang.System.exit;

public class Batchinsert {

    public static void checkArgs(String[] args) {

        if(args.length < 3 )
        {
            System.out.println("Usage: BatchInsert datafile indexoption rdfdbname");
            exit(0);
        }

        int indexoption;
        try {
            indexoption = Integer.parseInt(args[1]);
            if(indexoption > 5 || indexoption < 0)
            {
                System.err.println("Index out of range (1-5)");
                exit(0);
            }
        } catch (NumberFormatException e) {
            System.err.println("Indexoption must be an integer");
            exit(0);
        }

        String datafile = args[0];
        File file = new File(datafile);
        if(!file.exists())
        {
            System.err.println("File "+ datafile +" does not exist");
            exit(0);
        }
    }

    public static boolean createOrOpenDb(String dbname, int indexoption) {
        SystemDefs sysdefs = null;
        File dbfile = new File(dbname); //Check if database already exists
        boolean dbexists = dbfile.exists();
        if(!dbexists) {
            sysdefs = new SystemDefs(dbname, 100000, 1000, "Clock", indexoption);
        } else {
            sysdefs = new SystemDefs(dbname, 0, 1000, "Clock", indexoption);
        }
        SystemDefs.JavabaseDB.openDB(dbname, 10000000);
        return dbexists;
    }

    public static void processBatchInsert(File datafile, int indexoption, String dbname) throws InvalidPageNumberException, IOException, FileIOException, DiskMgrException {

        boolean dbexists = createOrOpenDb(dbname, indexoption);

        Scanner scanner = new Scanner(datafile);

        while(scanner.hasNext()){

            try {
                String[] tokens = scanner.nextLine().replaceAll(":", "").split("\\s+");
                if(tokens.length < 4) {
                    System.err.print(String.join(" ", tokens) + ": ");
                    throw new Exception();
                }

                if (tokens[0] instanceof String && tokens[1] instanceof String && tokens[2] instanceof String) {
                    if (tokens[0].length() <=0 || tokens[1].length() <=0 || tokens[2].length() <= 0) {
                        System.err.print(String.join(" ", tokens) + ": ");
                        throw new Exception();
                    }
                    Double.parseDouble(tokens[3]);
                }

                try
                {
                    SystemDefs.JavabaseDB.insertNewQuadruple(tokens);
                }
                catch (Exception e) {
                    System.err.println("Insert Quadruple into Temporary Heapfile failed.");
                    e.printStackTrace();
                }
            }
            catch (Exception e) {
                System.err.println("Bad Record.");
                continue;
            }

        }

        SystemDefs.JavabaseDB.sortAndInsertQuadruples(dbexists, indexoption);
    }

    public static void main(String[] args) throws InvalidPageNumberException, IOException, FileIOException, DiskMgrException {

        checkArgs(args);

        File datafile = new File(args[0]);
        int indexoption = Integer.parseInt(args[1]);
        String dbname = new String("/tmp/"+args[2]+"."+indexoption);

        int iread = PCounter.rCounter;
        int iwrite = PCounter.wCounter;

        processBatchInsert(datafile, indexoption, dbname);

        // Following order as implemented for orderType + one index
        System.out.print(" Create BTree Index on ");
        switch(indexoption) {
            case 1:
                System.out.println(" subject and confidence: ");
                SystemDefs.JavabaseDB.index_subject_confidence();
                break;
            case 2:
                System.out.println(" predicate and confidence: ");
                SystemDefs.JavabaseDB.index_predicate_confidence();
                break;
            case 3:
                System.out.println(" 3. object and confidence: ");
                SystemDefs.JavabaseDB.index_object_confidence();
                break;

            case 4:
                System.out.println(" 4. confidence: ");
                SystemDefs.JavabaseDB.index_confidence();
                break;

            case 5:
                System.out.println(" 5. subject: ");
                SystemDefs.JavabaseDB.index_subject();
                break;
        }

        int fread = PCounter.rCounter;
        int fwrite = PCounter.wCounter;

        System.out.println("Disk Page Stats: ");
        System.out.println("Total Page Reads "+ (fread - iread));
        System.out.println("Total Page Writes "+ (fwrite - iwrite));

        SystemDefs.close();
    }

}
