package tests;

import diskmgr.PCounter;
import global.*;

import java.io.*;
import quadrupleheap.*;
import java.lang.*;
import labelheap.*;

import static global.GlobalConst.NUMBUF;
import static java.lang.System.exit;

public class Report
{
    public static void main(String[] args)
    {
        SystemDefs sysdef = null;
        String dbname = null;   //Database name
        int indexoption = 0;    //Index option
        int iread = PCounter.rCounter;
        int iwrite = PCounter.wCounter;
        if(args.length == 2 )   //Check if the args are RDFDBNAME INDEXOPTION
        {
            try{
                indexoption = Integer.parseInt(args[1]);
            }
            catch(Exception e){
                System.err.println("Index option must be an integer");
                exit(0);
            }

            dbname = new String("/tmp/"+args[0]+"."+indexoption);

            if(indexoption>5 || indexoption<0)
            {
                System.err.println("Indexoption only allowed within range: 1 to 5");
                exit(0);
            }

        }
        else
        {
            System.err.println("Usage:report RDFDBNAME INDEXOPTION");
            exit(0);
        }

        File dbfile = new File(dbname); //Check if database already exsist
        if(dbfile.exists())
        {
            //Database present. Opening existing database
            sysdef = new SystemDefs(dbname, 0, NUMBUF, "Clock", indexoption);
            System.out.println("*** Opening existing database ***");
        }
        else
        {
            System.out.println("*** " + dbname + " Does Not Exist ***");
            return;
        }

        try
        {
            QuadrupleHeapFile qhf = SystemDefs.JavabaseDB.getQuadrupleHandle();
            LabelHeapFile elhf = SystemDefs.JavabaseDB.getEntityHandle();
            LabelHeapFile plhf = SystemDefs.JavabaseDB.getPredicateHandle();
            System.out.println("\n\n\nReport - RDF DB Statistics");
            System.out.println(" RDF Database Name		: " + dbname);
            System.out.println(" DB Size 			: " + dbfile.length() + " bytes");
            System.out.println(" Page Size 			: " + SystemDefs.JavabaseDB.db_page_size() + " bytes");
            System.out.println(" Number of Pages in DB 		: " + SystemDefs.JavabaseDB.db_num_pages());
            System.out.println(" Quadruple Size 	        : " + GlobalConst.MINIBASE_QUADRUPLESIZE + " bytes");
            System.out.println(" Total Entities 		: " + SystemDefs.JavabaseDB.getEntityCnt());
            System.out.println(" Total Subjects 		: " + SystemDefs.JavabaseDB.getSubjectCnt());
            System.out.println(" Total Predicates 		: " + SystemDefs.JavabaseDB.getPredicateCnt());
            System.out.println(" Total Objects 			: " + SystemDefs.JavabaseDB.getObjectCnt());
            System.out.println(" Total Quadruples		: " + SystemDefs.JavabaseDB.getQuadrupleCnt());
            System.out.println(" Page Replacement Policy 	: Clock");
            System.out.println("\n --------- Heap Files ---------");
            System.out.println(" Quadruple File Name		: " + dbname + "/quadrupleHF");
            System.out.println(" Quadruple File Record Count	: " + qhf.getQuadrupleCnt());
            System.out.println(" Entity File Name		: " + dbname + "/entityHF");
            System.out.println(" Entity File Record Count	: " + elhf.getLabelCnt());
            System.out.println(" Predicate File Name		: " + dbname + "/predicateHF");
            System.out.println(" Predicate File Record Count	: " + plhf.getLabelCnt());
            System.out.println("\n --------- Page Read and Write  ---------");
            System.out.println(" Total Page Reads               : " + String.valueOf(PCounter.rCounter) );
            System.out.println(" Total Page Writes              : " + String.valueOf(PCounter.wCounter) );
            System.out.println(" ------------------------------");
            System.out.println("****************************************************************\n\n\n");
        }
        catch(Exception e)
        {
            System.out.println(e);
        }
        SystemDefs.close();
    }
}
