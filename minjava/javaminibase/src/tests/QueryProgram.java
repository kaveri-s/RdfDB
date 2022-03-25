package tests;

import diskmgr.*;
import global.*;

import java.io.*;

import quadrupleheap.*;

import static global.GlobalConst.NUMBUF;
import static java.lang.System.exit;


public class QueryProgram {
    public static SystemDefs sysdef = null;
    static String dbname = null;   //Database name
    static String Subject = null;
    static String Object = null;
    static String Predicate = null;
    static String Confidence = null;
    static int indexoption = 1;
    static int order = 1;//Index option
    public static double confidence = -99.0;
    public static int num_of_buf = 1000;


    public static void checkArgs(String[] args){

        if(args.length<8){
            System.err.println("Usage:query DATABASENAME INDEXOPTION SORTORDER SUBJECTFILTER PREDICATEFILTER OBJECTFILTER CONFIDENCEFILTER NUMBUF***");
            exit(0);
        }
        try{
            indexoption = Integer.parseInt(args[1]);
        }
        catch(Exception e){
            System.err.println("Index option must be an integer");
            exit(0);
        }
        try{
            order= Integer.parseInt(args[2]);
        }
        catch(Exception e){
            System.err.println("Order must be an integer");
            exit(0);
        }
        try{
            num_of_buf = Integer.parseInt(args[7]);
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

        if(indexoption>5 || indexoption<0)
        {
            System.out.println("Sortoption only allowed within range: 1 to 5");
            exit(0);
        }

        dbname = new String("/tmp/"+args[0]+"."+indexoption);
    }
    public static void main(String[] args)
            throws Exception
    {

        System.out.println(String.join(" ", args));
        checkArgs(args);
        int init_read=PCounter.rCounter;
        int init_write=PCounter.wCounter;

        indexoption = Integer.parseInt(args[1]);
        dbname = new String("/tmp/"+args[0]+"."+indexoption);
        order= Integer.parseInt(args[2]);
        Subject = new String(args[3]).replaceAll(":", "");
        Predicate = new String(args[4]).replaceAll(":", "");
        Object = new String(args[5]).replaceAll(":", "");
        Confidence = new String(args[6]).replaceAll(":", "");
        num_of_buf = Integer.parseInt(args[7]);
        if(Confidence.compareToIgnoreCase("*") != 0)
        {
            confidence = Double.parseDouble(Confidence);
        }

        File dbfile = new File(dbname); //Check if database already exist
        if(dbfile.exists())
        {
            //Database present. Opening existing database
            System.out.println("Database already present. Opening it");
            sysdef = new SystemDefs(dbname,0,1000,"Clock",indexoption);
//            SystemDefs.JavabaseDB.openDB(dbname);

        }
        else
        {
            System.out.println("*** Database does not exist ***");
            return;
        }

        Stream s = SystemDefs.JavabaseDB.openStream(order, Subject, Predicate, Object, confidence,num_of_buf);
        Quadruple quad = new Quadruple();
        QID qid = new QID();
        while((quad = s.getNext(qid))!=null)
        {
            quad.print();
        }
        if(s!=null)
        {
            s.closeStream();
        }
        SystemDefs.close();

        int fin_read=PCounter.rCounter;
        int fin_write=PCounter.wCounter;

        System.out.println("Total Page Writes "+ (fin_write-init_write));
        System.out.println("Total Page Reads "+ (fin_read-init_read));
    }
}
