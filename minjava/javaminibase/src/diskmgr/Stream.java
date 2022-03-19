/* File Stream.java */

package diskmgr;


import global.*;

import iterator.Sort;
import labelheap.Label;
import labelheap.LabelHeapFile;
import quadrupleheap.*;
import btree.*;

import static java.lang.System.exit;

public class Stream implements GlobalConst {
    rdfDB rdfDatabase;
    private static String dbName;
    int orderType;
    String subjectFilter;
    String predicateFilter;
    String objectFilter;
    double confidenceFilter;

    public static int sortOption = 1;

    public TScan tScan = null;


    static boolean subject_null = false;
    static boolean object_null = false;
    static boolean predicate_null = false;
    static boolean confidence_null = false;


    boolean scanEntireHeapFile = false;
    public boolean scan_on_BT = false;
    public Quadruple scan_on_BT_quadruple = null;

    public static QuadrupleHeapFile Result_HF = null;


    public static EID entityObjectId = new EID();
    public static EID entitySubjectId = new EID();
    public static PID entityPredicateId = new PID();

    // todo Iterator.Sort
    public Sort tSort = null;

    Stream(){ }

    public Stream(rdfDB rdfDatabase, int orderType, String subjectFilter, String predicateFilter, String objectFilter,
                  double confidenceFilter) throws Exception {

        this.orderType = orderType;
        this.subjectFilter = subjectFilter;
        this.predicateFilter = predicateFilter;
        this.objectFilter = objectFilter;
        this.confidenceFilter = confidenceFilter;

        sortOption = orderType;
        this.rdfDatabase = rdfDatabase;

        if(subjectFilter.compareToIgnoreCase("null") == 0)
        {
            subject_null = true;
        }
        if(predicateFilter.compareToIgnoreCase("null") == 0)
        {
            predicate_null = true;
        }
        if(objectFilter.compareToIgnoreCase("null") == 0)
        {
            object_null = true;
        }
        if(confidenceFilter == 0.00)
        {
            confidence_null = true;
        }
        dbName = rdfDatabase.getRdfDBName();
        String indexOption = dbName.substring(dbName.lastIndexOf('_') + 1);

        if(!subject_null && !predicate_null && !object_null && !confidence_null)
        {
            ScanBTReeIndex(subjectFilter,predicateFilter,objectFilter,confidenceFilter);
            scan_on_BT = true;
        }
        else
        {
            if(Integer.parseInt(indexOption) == 1 && !confidence_null)
            {
                ScanBTConfidenceIndex(subjectFilter,predicateFilter,objectFilter,confidenceFilter);
            }
            else if(Integer.parseInt(indexOption) == 2 && !subject_null && !confidence_null)
            {
                streamBySubjectConfidence(subjectFilter,predicateFilter,objectFilter,confidenceFilter);
            }
            else if(Integer.parseInt(indexOption) == 3 && !object_null && !confidence_null)
            {
                streamByObjectConfidence(subjectFilter,predicateFilter,objectFilter,confidenceFilter);
            }
            else if(Integer.parseInt(indexOption) == 4 && !predicate_null && !confidence_null)
            {
                streamByPredicateConfidence(subjectFilter,predicateFilter,objectFilter,confidenceFilter);
            }
            else if(Integer.parseInt(indexOption) == 5 && !subject_null)
            {
                ScanBTSubjectIndex(subjectFilter,predicateFilter,objectFilter,confidenceFilter);
            }
            else
            {
                scanEntireHeapFile = true;
                ScanEntireHeapFile(subjectFilter,predicateFilter,objectFilter,confidenceFilter);
            }

            //Sort the results
            tScan = new TScan(Result_HF);
            QuadrupleOrder sort_order = getSortOrder(orderType);
            try
            {
                // To do use Iterator.Sort() constructor here
                tSort = new Sort();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    public void closeStream() {
        try
        {
            if(tScan !=null)
            {
                tScan.closescan();
            }
            if(Result_HF != null && Result_HF != SystemDefs.JavabaseDB.getQuadrupleHandle())
            {
                Result_HF.deleteFile();
            }
            if(tSort !=null)
            {
                tSort.close(); //Close the stream iterator
            }
        }
        catch(Exception e)
        {
            System.out.println("Error closing Stream"+e);
        }
    }

    public Quadruple getNext(QID qid) {
        try
        {
            Quadruple quadruple;
            if(scan_on_BT)
            {
                if(scan_on_BT_quadruple!=null)
                {
                    Quadruple temp = new Quadruple(scan_on_BT_quadruple);
                    scan_on_BT_quadruple = null;
                    return temp;
                }
            }
            else
            {
                // Replace with Iterator.Sort for Quadruple
                while((quadruple = (Quadruple) tSort.get_next()) != null)
                {
                    if(!scanEntireHeapFile)
                    {
                        return quadruple;
                    }
                    else
                    {
                        boolean result = true;
                        double confidence = quadruple.getConfidence();
                        Label subject = SystemDefs.JavabaseDB.getEntityHandle().getLabel(quadruple.getSubjecqid().returnLID());
                        Label object = SystemDefs.JavabaseDB.getEntityHandle().getLabel(quadruple.getObjecqid().returnLID());
                        Label predicate = SystemDefs.JavabaseDB.getPredicateHandle().getLabel(quadruple.getPredicateID().returnLID());

                        if(!subject_null)
                        {
                            result = result & (subjectFilter.compareTo(subject.getLabel()) == 0);
                        }
                        if(!object_null)
                        {
                            result = result & (objectFilter.compareTo(object.getLabel()) == 0 );
                        }
                        if(!predicate_null)
                        {
                            result = result & (predicateFilter.compareTo(predicate.getLabel())==0);
                        }
                        if(!confidence_null)
                        {
                            result = result & (confidence >= confidenceFilter);
                        }
                        if(result)
                        {
                            return quadruple;
                        }
                    }
                }
            }
        }
        catch(Exception e)
        {
            System.out.println("Error in Stream get next\n"+e);
        }
        return null;
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
            default:
                System.err.println("RuntimeError. Sort order out of range (1-6). Welp, shouldn't be here");
                exit(1);
        }
        return sort_order;
    }

    public static LID GetEID(String filter) throws GetFileEntryException, PinPageException, ConstructPageException
    {
        LID eid = null;
        LabelBTreeFile entityBTree = new LabelBTreeFile(dbName+"/entityBT");
        KeyClass low_key = new StringKey(filter);
        KeyClass high_key = new StringKey(filter);
        KeyDataEntry entry;
        try
        {
            //Start Scanning BTree to check if subject is present
            LabelBTFileScan scan = entityBTree.new_scan(low_key,high_key);
            entry = scan.get_next();
            if(entry!=null)
            {
                eid =  ((LabelLeafData)entry.data).getData();
                scan.DestroyBTreeFileScan();
            }
            else
            {
                System.out.println("No Quadruple found with given criteria");
            }
            entityBTree.close();
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }

        return eid;
    }

    public static LID GetPredicate(String predicateFilter)
    {
        LID predicateId = null;
        ///Get the entity key from the Predicate BTREE file
        LabelBTreeFile Predicate_BTree;
        try
        {
            Predicate_BTree = new LabelBTreeFile(dbName+"/predicateBT");
            KeyClass low_key = new StringKey(predicateFilter);
            KeyClass high_key = new StringKey(predicateFilter);
            KeyDataEntry entry;

            //Start Scanning Btree to check if subject is present
            LabelBTFileScan scan1 = Predicate_BTree.new_scan(low_key,high_key);
            entry = scan1.get_next();
            if(entry!=null)
            {
                //return already existing EID ( convert lid to EID)
                predicateId =  ((LabelLeafData)entry.data).getData();
            }
            else
            {
                System.err.println("Predicate not present");
            }
            scan1.DestroyBTreeFileScan();
            Predicate_BTree.close();
        }
        catch(Exception e)
        {
            System.err.println("Predicate not present");

        }
        return predicateId;
    }


    public boolean ScanBTReeIndex(String Subject,String Predicate,String Object,double Confidence)
            throws Exception
    {
        if(GetEID(Subject) != null)
        {
            entitySubjectId = GetEID(Subject).returnEID();
        }
        else
        {
            System.out.println("No Quadruple found");
            return false;
        }

        ///Get the object key from the Entity BTREE file
        if(GetEID(Object)!=null)
        {
            entityObjectId = GetEID(Object).returnEID();
        }
        else
        {
            System.out.println("No Quadruple found");
            return false;
        }

        if(GetPredicate(Predicate) != null)
        {
            entityPredicateId = GetPredicate(Predicate).returnPID();
        }
        else
        {
            System.out.println("No Quadruple found");
            return false;
        }

        ///Get the entity key from the Predicate BTREE file
        //Compute the composite key for the QuadrupleBTREE search
        String key =  entitySubjectId.slotNo + ":" + entitySubjectId.pageNo.pid + ":" + entityPredicateId.slotNo + ":" + entityPredicateId.pageNo.pid + ":"
                + entityObjectId.slotNo + ":" + entityObjectId.pageNo.pid;
        KeyClass low_key = new StringKey(key);
        KeyClass high_key = new StringKey(key);
        KeyDataEntry entry;

        //Start Scanning BTree to check if  predicate already present
        QuadrupleHeapFile quadrupleHeapFile = SystemDefs.JavabaseDB.getQuadrupleHandle();
        QuadBTreeFile quadBTreeFile = SystemDefs.JavabaseDB.getQuadBTree();

        QuadBTFileScan scan = quadBTreeFile.new_scan(low_key,high_key);
        entry = scan.get_next();
        if(entry != null)
        {
            if(key.compareTo(((StringKey)(entry.key)).getKey()) == 0)
            {
                //return already existing TID
                QID quadrupleId = ((QuadLeafData)(entry.data)).getData();
                Quadruple record = quadrupleHeapFile.getQuadruple(quadrupleId);
                double orig_confidence = record.getConfidence();
                if(orig_confidence >= Confidence)
                {
                    scan_on_BT_quadruple = new Quadruple(record);
                }
            }
        }
        scan.DestroyBTreeFileScan();
        quadBTreeFile.close();
        return true;
    }

    private void ScanBTConfidenceIndex(String subjectFilter,String predicateFilter, String objectFilter, double confidenceFilter)
            throws Exception
    {
        boolean result;
        KeyDataEntry entry1;
        QID quadrupleId;
        Label subject, object, predicate;
        Quadruple record;

        QuadBTreeFile quadBTreeFile = SystemDefs.JavabaseDB.getQuadBTree();
        QuadrupleHeapFile quadrupleHeapFile = SystemDefs.JavabaseDB.getQuadrupleHandle();
        LabelHeapFile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapFile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();

        java.util.Date date= new java.util.Date();
        Result_HF = new QuadrupleHeapFile(Long.toString(date.getTime()));

        KeyClass low_key1 = new StringKey(Double.toString(confidenceFilter));
        QuadBTFileScan scan = quadBTreeFile.new_scan(low_key1,null);

        while((entry1 = scan.get_next())!= null)
        {
            result = true;

            quadrupleId =  ((QuadLeafData)entry1.data).getData();
            record = quadrupleHeapFile.getQuadruple(quadrupleId);
            subject = Entity_HF.getLabel(record.getSubjecqid().returnLID());
            object = Entity_HF.getLabel(record.getObjecqid().returnLID());
            predicate = Predicate_HF.getLabel(record.getPredicateID().returnLID());

            if(!subject_null)
            {
                result = result & (subjectFilter.compareTo(subject.getLabel()) == 0);
            }
            if(!object_null)
            {
                result = result & (objectFilter.compareTo(object.getLabel()) == 0 );
            }
            if(!predicate_null)
            {
                result = result & (predicateFilter.compareTo(predicate.getLabel())==0);
            }
            if(!confidence_null)
            {
                result = result & (record.getConfidence() >= confidenceFilter);
            }

            if(result)
            {
                System.out.println("Subject::" + subject.getLabel()+ "\tPredicate::"+predicate.getLabel() + "\tObject::"+object.getLabel() );
                Result_HF.insertQuadruple(record.returnTupleByteArray());
            }
        }
        scan.DestroyBTreeFileScan();
        quadBTreeFile.close();
    }


    private void streamBySubjectConfidence(String subjectFilter,String predicateFilter,String objectFilter,double confidenceFilter)
    {
        try
        {
            QuadBTreeFile quadBTreeFile = SystemDefs.JavabaseDB.getQuadBTree();

            QuadrupleHeapFile quadrupleHeapFile = SystemDefs.JavabaseDB.getQuadrupleHandle();
            LabelHeapFile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
            LabelHeapFile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();
            java.util.Date date= new java.util.Date();
            Result_HF = new QuadrupleHeapFile(Long.toString(date.getTime()));

            KeyClass low_key = new StringKey(subjectFilter+":"+confidenceFilter);
            KeyDataEntry entry;
            double orig_confidence;
            Quadruple record;
            Label subject, object, predicate;
            boolean result;

            QuadBTFileScan scan = quadBTreeFile.new_scan(low_key,null);

            QID qId;
            while((entry = scan.get_next())!= null)
            {
                qId= ((QuadLeafData)entry.data).getData();
                record = quadrupleHeapFile.getQuadruple(qId);
                orig_confidence = record.getConfidence();

                subject = Entity_HF.getLabel(record.getSubjecqid().returnLID());
                object = Entity_HF.getLabel(record.getObjecqid().returnLID());
                predicate = Predicate_HF.getLabel(record.getPredicateID().returnLID());
                result = true;

                if(!subject_null)
                {
                    result = result & (subjectFilter.compareTo(subject.getLabel()) == 0);
                }
                if(!object_null)
                {
                    result = result & (objectFilter.compareTo(object.getLabel()) == 0 );
                }
                if(!predicate_null)
                {
                    result = result & (predicateFilter.compareTo(predicate.getLabel())==0);
                }
                if(!confidence_null)
                {
                    result = result & (orig_confidence >= confidenceFilter);
                }
                if(subjectFilter.compareTo(subject.getLabel()) != 0)
                {
                    System.out.println("Found next subject hence stopping");
                    break;
                }
                else if(result)
                {
                    System.out.println("Subject::" + subject.getLabel()+ "\tPredicate::"+predicate.getLabel() + "\tObject::"+object.getLabel() );
                    Result_HF.insertQuadruple(record.returnTupleByteArray());
                }
            }
            scan.DestroyBTreeFileScan();
            quadBTreeFile.close();
        }
        catch(Exception e)
        {
            System.err.println ("Error for subject and confidence index query"+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

    }


    private void streamByObjectConfidence(String subjectFilter,String predicateFilter,String objectFilter,double confidenceFilter)
    {
        try
        {
            QuadBTreeFile quadBTreeFile = SystemDefs.JavabaseDB.getQuadBTree();
            QuadrupleHeapFile quadrupleHeapFile = SystemDefs.JavabaseDB.getQuadrupleHandle();
            LabelHeapFile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
            LabelHeapFile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();

            java.util.Date date= new java.util.Date();
            Result_HF = new QuadrupleHeapFile(Long.toString(date.getTime()));

            KeyClass low_key = new StringKey(objectFilter+":"+confidenceFilter);
            KeyDataEntry entry;

            QuadBTFileScan scan = quadBTreeFile.new_scan(low_key,null);
            QID qId;
            Label subject, object, predicate;
            Quadruple record;
            double orig_confidence;
            boolean result;
            while((entry = scan.get_next())!= null)
            {
                qId=  ((QuadLeafData)entry.data).getData();

                record = quadrupleHeapFile.getQuadruple(qId);
                orig_confidence = record.getConfidence();

                subject = Entity_HF.getLabel(record.getSubjecqid().returnLID());
                object = Entity_HF.getLabel(record.getObjecqid().returnLID());
                predicate = Predicate_HF.getLabel(record.getPredicateID().returnLID());

                result = true;

                if(!subject_null)
                {
                    result = result & (subjectFilter.compareTo(subject.getLabel()) == 0);
                }
                if(!object_null)
                {
                    result = result & (objectFilter.compareTo(object.getLabel()) == 0 );
                }
                if(!predicate_null)
                {
                    result = result & (predicateFilter.compareTo(predicate.getLabel())==0);
                }
                if(!confidence_null)
                {
                    result = result & (orig_confidence >= confidenceFilter);
                }
                if(objectFilter.compareTo(object.getLabel()) != 0)
                {
                    System.out.println("Found next object hence stopping");
                    break;
                }
                else if(result)
                {
                    System.out.println("Inserting "+object.getLabel()+confidenceFilter);
                    Result_HF.insertQuadruple(record.returnTupleByteArray());
                }
            }
            scan.DestroyBTreeFileScan();
            quadBTreeFile.close();
        }
        catch(Exception e)
        {
            System.err.println ("Error for object and confidence index query"+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }


    private void streamByPredicateConfidence(String subjectFilter,String predicateFilter,String objectFilter,double confidenceFilter)
    {
        try
        {
            QuadBTreeFile quadBTreeFile = SystemDefs.JavabaseDB.getQuadBTree();
            QuadrupleHeapFile quadrupleHeapFile = SystemDefs.JavabaseDB.getQuadrupleHandle();
            LabelHeapFile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
            LabelHeapFile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();
            java.util.Date date= new java.util.Date();
            Result_HF = new QuadrupleHeapFile(Long.toString(date.getTime()));


            KeyClass low_key = new StringKey(predicateFilter+":"+confidenceFilter);
            QuadBTFileScan scan = quadBTreeFile.new_scan(low_key,null);
            KeyDataEntry entry;
            QID qId;
            Label subject, object, predicate;
            Quadruple record;
            double orig_confidence;
            boolean result;

            while((entry = scan.get_next())!= null)
            {
                qId=  ((QuadLeafData)entry.data).getData();
                System.out.println("Quadruple found : " + ((StringKey)(entry.key)).getKey() + "qId" + qId);
                record = quadrupleHeapFile.getQuadruple(qId);
                orig_confidence = record.getConfidence();

                subject = Entity_HF.getLabel(record.getSubjecqid().returnLID());
                object = Entity_HF.getLabel(record.getObjecqid().returnLID());
                predicate = Predicate_HF.getLabel(record.getPredicateID().returnLID());

                result = true;

                if(!subject_null)
                {
                    result = result & (subjectFilter.compareTo(subject.getLabel()) == 0);
                }
                if(!object_null)
                {
                    result = result & (objectFilter.compareTo(object.getLabel()) == 0 );
                }
                if(!predicate_null)
                {
                    result = result & (predicateFilter.compareTo(predicate.getLabel())==0);
                }
                if(!confidence_null)
                {
                    result = result & (orig_confidence >= confidenceFilter);
                }
                if(predicateFilter.compareTo(predicate.getLabel()) != 0)
                {
                    System.out.println("Found next predicate hence stopping");
                    break;
                }
                else if(result)
                {
                    System.out.println("Inserting "+object.getLabel()+confidenceFilter);
                    Result_HF.insertQuadruple(record.returnTupleByteArray());
                }
            }
            scan.DestroyBTreeFileScan();
            quadBTreeFile.close();
        }
        catch(Exception e)
        {
            System.err.println ("Error for predicate and confidence index query"+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }


    public static void ScanBTSubjectIndex(String subjectFilter,String predicateFilter, String objectFilter, double confidenceFilter)
            throws Exception
    {
        QuadBTreeFile quadBTreeFile = SystemDefs.JavabaseDB.getQuadBTree();
        QuadrupleHeapFile quadrupleHeapFile = SystemDefs.JavabaseDB.getQuadrupleHandle();
        LabelHeapFile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapFile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();
        java.util.Date date= new java.util.Date();
        Result_HF = new QuadrupleHeapFile(Long.toString(date.getTime()));

        QID qId;
        KeyClass low_key = new StringKey(subjectFilter);
        KeyClass high_key = new StringKey(subjectFilter);
        KeyDataEntry entryConf;
        Quadruple record;
        Label subject, object, predicate;
        //Start Scanning Btree to check if subject is present
        QuadBTFileScan scan = quadBTreeFile.new_scan(low_key,high_key);
        entryConf = scan.get_next();

        try
        {
            while(entryConf!=null)
            {
                boolean result = true;
                qId =  ((QuadLeafData)entryConf.data).getData();
                record = quadrupleHeapFile.getQuadruple(qId);

                subject = Entity_HF.getLabel(record.getSubjecqid().returnLID());
                object = Entity_HF.getLabel(record.getObjecqid().returnLID());
                predicate = Predicate_HF.getLabel(record.getPredicateID().returnLID());

                if(!object_null)
                {
                    result = result & (object.getLabel().compareTo(objectFilter)==0);
                }
                if(!predicate_null)
                {
                    result = result & (predicate.getLabel().compareTo(predicateFilter)==0);
                }
                System.out.println(subject.getLabel()+" "+predicate.getLabel()+" "+object.getLabel()+" "+record.getConfidence());
                if(!confidence_null)
                {
                    result = result & (record.getConfidence() >= confidenceFilter);
                }
                if(result)
                {
                    System.out.println("Subject found");
                    Result_HF.insertQuadruple(record.returnTupleByteArray());
                }
                entryConf = scan.get_next();
            }
        }
        catch(Exception ex)
        {
            System.out.println("Exception::"+ex);
        }

        scan.DestroyBTreeFileScan();
        quadBTreeFile.close();
    }

    private void ScanEntireHeapFile(String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter)
    {
        try
        {
            this.subjectFilter = subjectFilter;
            this.predicateFilter = predicateFilter;
            this.objectFilter =  objectFilter;
            this.confidenceFilter = confidenceFilter;
            Result_HF = SystemDefs.JavabaseDB.getQuadrupleHandle();
        }
        catch(Exception e)
        {
            System.err.println ("Error scanning entire heap file for query::"+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

}