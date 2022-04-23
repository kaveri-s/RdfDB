package tests;

import diskmgr.*;
import global.*;

import java.io.*;
import java.sql.Array;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Stack;
import java.util.regex.Pattern;

import quadrupleheap.*;

import static global.GlobalConst.NUMBUF;
import static java.lang.System.exit;


public class QueryProgram {
    public static SystemDefs sysdef = null;
    static String dbname = null;   //Database name
    static String queryfile = null;
    public static int num_of_buf = 1000;

    public static void checkArgs(String[] args){

        if(args.length<3){
            System.err.println("Usage:query DATABASENAME QUERYFILE NUMBUF");
            exit(0);
        }
        try{
            dbname= "/tmp/"+args[0];
            File dbfile = new File(dbname);
            if(dbfile.exists())
            {
                System.out.println("Database already present. Opening it");
            }
            else
            {
                System.err.println("Database does not exist");
                return;
            }
        }
        catch (Exception e){
            System.err.println("Error opening file");
            exit(0);
        }
        try{
            queryfile = args[1];
            File file = new File(queryfile);
            if(!file.exists())
            {
                System.err.println("File "+ queryfile +" does not exist");
                exit(0);
            }
        }
        catch(Exception e){
            System.err.println("Query file error");
            exit(0);
        }
        try{
            num_of_buf = Integer.parseInt(args[2]);
        }
        catch(Exception e){
            System.err.println("Number of buffers must be an integer");
            exit(0);
        }
        if(num_of_buf <= 0)
        {
            System.out.println("Invalid buffer option");
            exit(0);
        }
    }

    public static void parseAndRun() throws FileNotFoundException {
        File file = new File(queryfile);

        Scanner scanner = new Scanner(file).useDelimiter("\\Z");
        String query = scanner.next().replaceAll("[\\n\\t ]", "");
        scanner.close();

        String delimiters = ",|\\(|\\),|\\)";

        try {
            scanner = new Scanner(query).useDelimiter(delimiters);
            scanner.next("S");
            scanner.next("J");
            scanner.next("J");
            String SF1 = scanner.next().replace("[", "");
            String PF1 = scanner.next();
            String OF1 = scanner.next();
            double CF1 = Double.parseDouble(scanner.next().replace("]", ""));
            int JNP1 = Integer.parseInt(scanner.next("[0-1]"));
            int JONO1 = Integer.parseInt(scanner.next("[0-1]"));
            String RSF1 = scanner.next();
            String RPF1 = scanner.next();
            String ROF1 = scanner.next();
            double RCF1 = scanner.nextDouble();
            ArrayList<Integer> LONP1 = new ArrayList<Integer>();
            while(!scanner.hasNext("\\[?[0-3]\\]"))
                LONP1.add(Integer.parseInt(scanner.next("\\[?[0-3]").replace("[", "")));
            LONP1.add(Integer.parseInt(scanner.next("\\[?[0-3]\\]").replaceAll("[\\[\\]]", "")));
            int ORS1 = Integer.parseInt(scanner.next("[0-1]"));
            int ORO1 = Integer.parseInt(scanner.next("[0-1]"));
            int JNP2 = Integer.parseInt(scanner.next("[0-3]"));
            int JONO2 = Integer.parseInt(scanner.next("[0-1]"));
            String RSF2 = scanner.next();
            String RPF2 = scanner.next();
            String ROF2 = scanner.next();
            double RCF2 = scanner.nextDouble();
            ArrayList<Integer> LONP2 = new ArrayList<Integer>();
            while(!scanner.hasNext("\\[?[0-5]\\]"))
                LONP2.add(Integer.parseInt(scanner.next("\\[?[0-5]").replaceAll("[\\[\\]]", "")));
            LONP2.add(Integer.parseInt(scanner.next("\\[?[0-5]\\]").replaceAll("[\\[\\]]", "")));
            int ORS2 = Integer.parseInt(scanner.next("[0-1]"));
            int ORO2 = Integer.parseInt(scanner.next("[0-1]"));
            int SO = Integer.parseInt(scanner.next("[0-2]"));
            int SNP = Integer.parseInt(scanner.next("-1|[0-5]"));
            int NP = Integer.parseInt(scanner.next("[0-9]*"));
            try
            {

                System.out.printf("%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s\n", SF1 ,PF1, OF1, CF1,
                        JNP1, JONO1, RSF1, RPF1, ROF1, RCF1, LONP1, ORS1, ORO1, JNP2, JONO2, RSF2, RPF2, ROF2, RCF2, LONP2, ORS2, ORO2,
                        SO, SNP, NP);

                sysdef = new SystemDefs(dbname, 0, 1500, "Clock", 1);
                SystemDefs.JavabaseDB.executeQuery(num_of_buf, SF1, PF1, OF1, CF1, JNP1, JONO1, RSF1, RPF1, ROF1, RCF1, LONP1, ORS1, ORO1,
                       JNP2, JONO2, RSF2, RPF2, ROF2, RCF2, LONP2, ORS2, ORO2, SO, SNP, NP);
                SystemDefs.close();
            }
            catch (Exception e) {
                System.err.println("Query Execute failed.");
                e.printStackTrace();
            }
        }
        catch (Exception e) {
            System.err.println("Syntax Error Thrown");
            e.printStackTrace();
        }
        finally {
            scanner.close();
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        checkArgs(args);

        System.out.printf("%s %s %s\n", args[0], args[1], args[2]);

        int init_read=PCounter.rCounter;
        int init_write=PCounter.wCounter;

        parseAndRun();

        int fin_read=PCounter.rCounter;
        int fin_write=PCounter.wCounter;

        System.out.println("Total Page Writes "+ (fin_write-init_write));
        System.out.println("Total Page Reads "+ (fin_read-init_read));
    }
}
