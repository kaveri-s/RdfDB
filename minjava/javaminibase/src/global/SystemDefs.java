package global;

import bufmgr.*;
import diskmgr.*;
import catalog.*;

public class SystemDefs {
    public static BufMgr JavabaseBM;
    //public static DB	JavabaseDB;
    public static rdfDB JavabaseDB;
    public static Catalog JavabaseCatalog;

    public static String JavabaseDBName;
    public static String JavabaseLogName;
    public static boolean MINIBASE_RESTART_FLAG = false;
    public static String MINIBASE_DBNAME;

    public SystemDefs() {
    }

    ;

    public SystemDefs(String dbname, int num_pgs, int bufpoolsize,
                      String replacement_policy) {
        int logsize;

        String real_logname = new String(dbname);
        String real_dbname = new String(dbname);

        if (num_pgs == 0) {
            logsize = 500;
        } else {
            logsize = 3 * num_pgs;
        }

        if (replacement_policy == null) {
            replacement_policy = new String("Clock");
        }

        init(real_dbname, real_logname, num_pgs, logsize,
                bufpoolsize, replacement_policy);
    }

    public SystemDefs(String rdfdbname, int num_pgs, int bufpoolsize,String replacement_policy,int index)
    {
        int logsize;

        String real_logname = new String(rdfdbname);
        String real_dbname = new String(rdfdbname);

        System.out.println(rdfdbname);

        if (num_pgs == 0) {
            logsize = 500;
        }
        else {
            logsize = 3*num_pgs;
        }

        if (replacement_policy == null) {
            replacement_policy = new String("Clock");
        }

        init_rdfDB(real_dbname,real_logname, num_pgs, logsize, bufpoolsize, replacement_policy,index);
    }

    public void init(String dbname, String logname,
                     int num_pgs, int maxlogsize,
                     int bufpoolsize, String replacement_policy) {

        boolean status = true;
        JavabaseBM = null;
        JavabaseDB = null;
        JavabaseDBName = null;
        JavabaseLogName = null;
        JavabaseCatalog = null;

        try {
            JavabaseBM = new BufMgr(bufpoolsize, replacement_policy);
            //JavabaseDB = new DB();
            JavabaseDB = new rdfDB(1);
/*
	JavabaseCatalog = new Catalog(); 
*/
        } catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        JavabaseDBName = new String(dbname);
        JavabaseLogName = new String(logname);
        MINIBASE_DBNAME = new String(JavabaseDBName);

        // create or open the DB

        if ((MINIBASE_RESTART_FLAG) || (num_pgs == 0)) {//open an existing database
            try {
                JavabaseDB.openDB(dbname);
            } catch (Exception e) {
                System.err.println("" + e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
        } else {
            try {
                JavabaseDB.openDB(dbname, num_pgs);
                JavabaseBM.flushAllPages();
            } catch (Exception e) {
                System.err.println("" + e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
        }
    }

    public void init_rdfDB( String dbname, String logname,int num_pgs, int maxlogsize,int bufpoolsize, String replacement_policy, int index)
    {

        boolean status = true;
        JavabaseBM = null;
        JavabaseDB = null;
        JavabaseDBName = null;
        JavabaseLogName = null;
        JavabaseCatalog = null;

        try {
            JavabaseBM = new BufMgr(bufpoolsize, replacement_policy);
            JavabaseDB = new rdfDB(index);
			/*
			   JavabaseCatalog = new Catalog();
			 */
        }
        catch (Exception e) {
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        JavabaseDBName = new String(dbname);
        JavabaseLogName = new String(logname);
        MINIBASE_DBNAME = new String(JavabaseDBName);

        // create or open the DB

        if ((MINIBASE_RESTART_FLAG)||(num_pgs == 0)){//open an existing database
            try {
                System.out.println("***Opening existing database***");
                JavabaseDB.openDB(JavabaseDBName,index); //open exisiting rdf database
            }
            catch (Exception e) {
                System.err.println (""+e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
        }
        else {
            try {
                System.out.println("***Creating new database***");
                JavabaseDB.openDB(JavabaseDBName, num_pgs); //create a new rdf database
                JavabaseBM.flushAllPages();
            }
            catch (Exception e) {
                System.err.println (""+e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
        }
    }

    public static QuadrupleOrder getSortOrder(int orderType)
    {
        QuadrupleOrder sort_order = null;

        switch(orderType)
        {
            case 1:
                sort_order = new QuadrupleOrder(QuadrupleOrder.SubjectPredicateObjectConfidence);
                break;

            case 2:
                sort_order = new QuadrupleOrder(QuadrupleOrder.PredicateSubjectObjectConfidence);
                break;

            case 3:
                sort_order = new QuadrupleOrder(QuadrupleOrder.SubjectConfidence);
                break;

            case 4:
                sort_order = new QuadrupleOrder(QuadrupleOrder.PredicateConfidence);
                break;

            case 5:
                sort_order = new QuadrupleOrder(QuadrupleOrder.ObjectConfidence);
                break;

            case 6:
                sort_order = new QuadrupleOrder(QuadrupleOrder.Confidence);
                break;
        }
        return sort_order;
    }


    public static void close()
    {
        try
        {
            JavabaseBM.flushAllPages();
        }
        catch(Exception e)
        {
            System.out.println ("Flushing Pages: ***************");
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        try
        {
            JavabaseDB.closeDB();
        }
        catch(Exception e)
        {
            System.out.println ("Closing RDFDB: ***************");
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }
}
