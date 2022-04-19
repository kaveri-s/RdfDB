package diskmgr;

import basicpattern.BasicPattern;
import bpiterator.BPFileScan;
import bpiterator.BPSort;
import btree.*;
import global.*;
import heap.Heapfile;
import bpiterator.BP_Triple_Join;
import labelheap.Label;
import labelheap.LabelHeapFile;
import quadrupleheap.Quadruple;
import quadrupleheap.QuadrupleHeapFile;
import quadrupleheap.TScan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class rdfDB extends DB implements GlobalConst {
    private QuadrupleHeapFile quadrupleHeapFile;
    private LabelHeapFile entityLabelHeapFile;
    private LabelHeapFile predicateLabelHeapFile;

    private LabelBTreeFile entityBTree;
    private LabelBTreeFile predicateBTree;
    private QuadBTreeFile quadrupleBTree;

    private LabelBTreeFile distinctSubjectsBTree;
    private LabelBTreeFile distinctObjectsBTree;

    private int subjectsCount;
    private int objectsCount;
    private int predicatesCount;
    private int quadruplesCount;
    private int entitiesCount;
    private int indexType;
    private String rdfDBname;

    public rdfDB(int type) {
        indexType = type;
        subjectsCount = 0;
        objectsCount = 0;
        predicatesCount = 0;
        quadruplesCount = 0;
        entitiesCount = 0;
    }

    public void openDB(String dbName) {
        rdfDBname = dbName;
        try {
            super.openDB(rdfDBname);
            initializeRdfDB();
        } catch (Exception e) {
            System.err.println("Error while opening the rdfDb. " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

    }

    public void openDB(String dbName, int num_pages) {
        rdfDBname = dbName;
        try {
            super.openDB(rdfDBname, num_pages);
            initializeRdfDB();
        } catch (Exception e) {
            System.err.println("Error while opening the rdfDb. " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

    }

    private void initializeRdfDB() {
        try {
            quadrupleHeapFile = new QuadrupleHeapFile(rdfDBname + "/quadrupleHF");
            entityLabelHeapFile = new LabelHeapFile(rdfDBname + "/entityHF");
            predicateLabelHeapFile = new LabelHeapFile(rdfDBname + "/predicateHF");

            entityBTree = new LabelBTreeFile(rdfDBname + "/entityBT", AttrType.attrString, 255, 1);
            predicateBTree = new LabelBTreeFile(rdfDBname + "/predicateBT", AttrType.attrString, 255, 1);
            quadrupleBTree = new QuadBTreeFile(rdfDBname + "/quadBT", AttrType.attrString, 255, 1);
            distinctSubjectsBTree = new LabelBTreeFile(rdfDBname + "/distinctSubjBT", AttrType.attrString, 255, 1);
            distinctObjectsBTree = new LabelBTreeFile(rdfDBname + "/distinctObjBT", AttrType.attrString, 255, 1);

            System.out.println("Initialised Database");

        } catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

    }

    public String getRdfDBName() {
        return rdfDBname;
    }

    public  void read_page(PageId pageno, Page apage){
        try{
            super.read_page(pageno, apage);
        }catch(Exception e){
            System.err.println("Error while reading the page from the disk " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

    public void write_page(PageId pageno, Page apage){
        try{
            super.write_page(pageno, apage);
        }catch(Exception e){
            System.err.println("Error while writing a page to the disk. " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

    }
    public QuadBTreeFile getQuadBTree() {
        return quadrupleBTree;
    }

    public LabelHeapFile getEntityHandle() {
        return entityLabelHeapFile;
    }

    public LabelHeapFile getPredicateHandle() {
        return predicateLabelHeapFile;
    }

    public QuadrupleHeapFile getQuadrupleHandle() {
        return quadrupleHeapFile;
    }

    public int getQuadrupleCnt() {
        try {
            quadruplesCount = quadrupleHeapFile.getQuadrupleCnt();
        } catch (Exception e) {
            System.err.println("Error while fetching the quadruples count. " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return quadruplesCount;
    }

    public int getEntityCnt() {
        try {
            entitiesCount = entityLabelHeapFile.getLabelCnt();
        } catch (Exception e) {
            System.err.println("Error while fetching the quadruples count. " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return entitiesCount;
    }

    public int getPredicateCnt() {
        try {
            predicatesCount = predicateLabelHeapFile.getLabelCnt();
        } catch (Exception e) {
            System.err.println("Error while fetching the quadruples count. " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return predicatesCount;
    }

    public int getSubjectCnt() {
        subjectsCount = 0;
        KeyDataEntry entry = null;
        HashMap<String, Integer> map = new HashMap<>();
        try {

            TScan tScanner = new TScan(getQuadrupleHandle());
            QID qid = new QID();
            Quadruple aquad;
            while ((aquad = tScanner.getNext(qid)) != null) {
                int pageSlot = aquad.getSubjecqid().slotNo;
                int pid = aquad.getSubjecqid().pageNo.pid;
                String key = String.valueOf(pageSlot) + String.valueOf(pid);
                if(!map.containsKey(key)){
                    subjectsCount++;
                    map.put(key, 1);
                }
            }
            tScanner.closescan();
        } catch (Exception e) {
            System.out.println("Error while fetching the quadruples count. " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return subjectsCount;
    }

    public int getObjectCnt() {
        objectsCount = 0;
        KeyDataEntry entry = null;
        HashMap<String, Integer> map = new HashMap<>();
        try {
            TScan tScanner = new TScan(getQuadrupleHandle());
            QID qid = new QID();
            Quadruple aquad;
            while ((aquad = tScanner.getNext(qid)) != null) {
                int pageSlot = aquad.getObjecqid().slotNo;
                int pid = aquad.getObjecqid().pageNo.pid;
                String key = String.valueOf(pageSlot) + String.valueOf(pid);
                if(!map.containsKey(key)){
                    objectsCount++;
                    map.put(key, 1);
                }
            }
            tScanner.closescan();
        } catch (Exception e) {
            System.err.println("Error while fetching the quadruples count. " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return objectsCount;
    }

    public EID insertEntity(String entityLabel) {
        LID lid = null;
        try {
            lid = insertLabel(entityLabel, entityBTree, entityLabelHeapFile);

        }catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return lid.returnEID();
    }

    public boolean deleteEntity(String entityLabel) {
        boolean result = false;
        try {
            result = deleteLabel(entityLabel, entityBTree, entityLabelHeapFile);
        }catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return result;
    }

    public PID insertPredicate(String predicateLabel) {
        LID lid = null;
        try {
            lid = insertLabel(predicateLabel, predicateBTree, predicateLabelHeapFile);

        }catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return lid.returnPID();
    }

    public boolean deletePredicate(String predicateLabel) {
        boolean result = false;
        try {
            result = deleteLabel(predicateLabel, predicateBTree, predicateLabelHeapFile);
        }catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return result;
    }

    public QID insertQuadruple(byte[] quadruplePtr) {
        QID qid = null;

        try {
            Quadruple temp = new Quadruple(quadruplePtr, 0);
            KeyClass key = getKeyForIndex(temp);
            double confidence = Convert.getFloValue(24, quadruplePtr);
            KeyClass low_key = key;
            KeyClass high_key = key;
            KeyDataEntry entry = null;

            QuadBTFileScan scan = quadrupleBTree.new_scan(low_key, high_key);
            entry = scan.get_next();

            if (entry != null && key == entry.key) {

                QID quadrupleID = ((QuadLeafData) (entry.data)).getData();
                Quadruple record = quadrupleHeapFile.getQuadruple(quadrupleID);
                double prevConfidence = record.getConfidence();
                if (prevConfidence < confidence) {
                    Quadruple newRecord = new Quadruple(quadruplePtr, 0);
                    quadrupleBTree.Delete(entry.key, quadrupleID);
                    quadrupleHeapFile.updateQuadruple(quadrupleID, newRecord);
                    quadrupleBTree.insert(low_key, quadrupleID);
                }
                scan.DestroyBTreeFileScan();
                return quadrupleID;
            }

            qid = quadrupleHeapFile.insertQuadruple(quadruplePtr);
            quadrupleBTree.insert(key, qid);
            scan.DestroyBTreeFileScan();

        } catch (Exception e) {
            System.err.println("Error while inserting the quadruples. " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return qid;
    }

    public boolean deleteQuadruple(byte[] quadruplePtr) {
        boolean isDeleteSuccessful = false;

        try {
            String key = getKeyFromQuadPtr(quadruplePtr);
            double confidence = Convert.getFloValue(24, quadruplePtr);
            KeyClass low_key = new StringKey(key);
            KeyClass high_key = new StringKey(key);
            KeyDataEntry entry = null;

            QuadBTFileScan scan = quadrupleBTree.new_scan(low_key, high_key);
            entry = scan.get_next();
            scan.DestroyBTreeFileScan();
            if (entry != null && key.compareTo(((StringKey) (entry.key)).getKey()) == 0) {
                QID quadrupleId = ((QuadLeafData) (entry.data)).getData();
                if (quadrupleId != null)
                    isDeleteSuccessful = quadrupleBTree.Delete(low_key, quadrupleId) && quadrupleHeapFile.deleteQuadruple(quadrupleId);

            }
        } catch (Exception e) {
            System.err.println("Error while deleting the quadruples. " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        return isDeleteSuccessful;
    }

    public Stream openStream(int orderType, String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter, int num_of_buf) {
        Stream streamObj = null;
        try {
            streamObj = new Stream(this, orderType, subjectFilter, predicateFilter, objectFilter, confidenceFilter, num_of_buf);
        } catch (Exception e) {
            System.err.println("Error while opening the stream. " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return streamObj;
    }

    public void insertNewQuadruple(String data[]) throws Exception {
        EID sid = insertEntity(data[0]);
        PID pid = insertPredicate(data[1]);
        EID oid = insertEntity(data[2]);
        double confidence = Double.parseDouble(data[3]);

        Quadruple quad = new Quadruple();
        quad.setSubjecqid(sid);
        quad.setPredicateid(pid);
        quad.setObjecqid(oid);
        quad.setConfidence(confidence);

        try {
            insertQuadruple(quad.getQuadrupleByteArray());
        } catch (Exception e) {
            System.err.println("Insert temp Quadruple failed.");
            e.printStackTrace();
        }
    }

    private KeyClass getKeyForIndex(Quadruple quad){
        KeyClass key = null;
        double confidence;
        Label subject;
        try{
            switch(indexType){
                case 1: // index on confidence
                    confidence = quad.getConfidence();
                    key = new StringKey(Double.toString(confidence));
                    break;

                case 2: // index on subject_confidence
                    confidence = quad.getConfidence();
                    subject = entityLabelHeapFile.getLabel(quad.getSubjecqid().returnLID());
                    key = new StringKey(subject.getLabel() + ":" + Double.toString(confidence));
                    break;

                case 3: //index on object_confidence
                    confidence = quad.getConfidence();
                    Label object = entityLabelHeapFile.getLabel(quad.getObjecqid().returnLID());
                    key = new StringKey(object.getLabel() + ":" + Double.toString(confidence));
                    break;
                    
                case 4: //index on predicate_confidence
                    confidence = quad.getConfidence();
                    Label predicate = predicateLabelHeapFile.getLabel(quad.getPredicateID().returnLID());
                    key = new StringKey(predicate.getLabel() + ":" + Double.toString(confidence));
                    break;

                case 5: // index on subject
                    subject = entityLabelHeapFile.getLabel(quad.getSubjecqid().returnLID());
                    key = new StringKey(subject.getLabel());
                    break;
            }

        }catch(Exception e){
            System.err.println("Error while creating key for indexing. " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return key;

    }

    private LID insertLabel(String label, LabelBTreeFile targetLabelBT, LabelHeapFile targetLabelHeapfile) {
        LID lid = null;
        KeyClass low_key = new StringKey(label);
        KeyClass high_key = new StringKey(label);
        KeyDataEntry entry = null;
        try {
            //Scan the btree to check if the entity already exists
            LabelBTFileScan scan = targetLabelBT.new_scan(low_key, high_key);
            entry = scan.get_next();

            if (entry != null && label.equals(((StringKey) (entry.key)).getKey())) { //return already existing EID

                lid = ((LabelLeafData) entry.data).getData();
                scan.DestroyBTreeFileScan();
                return lid;
            }
            scan.DestroyBTreeFileScan();
            lid = targetLabelHeapfile.insertLabel(label.getBytes());
            targetLabelBT.insert(new StringKey(label), lid);

        } catch (Exception e) {
            System.err.println("Error while inserting the label. " + e);
            e.printStackTrace();
        }

        return lid;

    }

    private boolean deleteLabel(String label, LabelBTreeFile targetLabelBT, LabelHeapFile targetLabelHeapfile) {
        boolean isDeleteSuccessful = false;
        LID lid = null;
        KeyClass low_key = new StringKey(label);
        KeyClass high_key = new StringKey(label);
        KeyDataEntry entry = null;
        try {
            LabelBTFileScan scan = targetLabelBT.new_scan(low_key, high_key);
            entry = scan.get_next();
            if (entry != null && label.equals(((StringKey) (entry.key)).getKey())) {
                lid = ((LabelLeafData) entry.data).getData();
                isDeleteSuccessful = targetLabelHeapfile.deleteLabel(lid) & targetLabelBT.Delete(low_key, lid);
            }
            scan.DestroyBTreeFileScan();
        } catch (Exception e) {
            System.err.println("Error while deleting the label. " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return isDeleteSuccessful;
    }

    private String getKeyFromQuadPtr(byte[] quadruplePtr) {
        String key = "";
        try {
            int subSlotNo = Convert.getIntValue(0, quadruplePtr);
            int subPageNo = Convert.getIntValue(4, quadruplePtr);
            int predSlotNo = Convert.getIntValue(8, quadruplePtr);
            int predPageNo = Convert.getIntValue(12, quadruplePtr);
            int objSlotNo = Convert.getIntValue(16, quadruplePtr);
            int objPageNo = Convert.getIntValue(20, quadruplePtr);
            double confidence = Convert.getFloValue(24, quadruplePtr);
            key = new String(Integer.toString(subSlotNo) + ':' + Integer.toString(subPageNo) + ':' +
                    Integer.toString(predSlotNo) + ':' + Integer.toString(predPageNo) + ':' +
                    Integer.toString(objSlotNo) + ':' + Integer.toString(objPageNo));
        } catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return key;
    }

    public void insertNewBasicPattern(Heapfile bpHeapfile, int num_nodes, EID[] nodes, double confidence) throws Exception {
        BasicPattern bp = new BasicPattern(num_nodes);
        bp.setConfidence(confidence);
        bp.setNodeIDs(num_nodes, nodes);

        try {
            bpHeapfile.insertRecord(bp.returnTupleByteArray());
        } catch (Exception e) {
            System.err.println("Insert Basic Pattern failed.");
            e.printStackTrace();
        }
    }


    public BPFileScan initBPScan(Heapfile heapfile, String SF, String PF, String OF, Double CF) throws Exception {
        TScan tScanner = new TScan(getQuadrupleHandle());
        QID qid = new QID();
        Quadruple aquad;
        while ((aquad = tScanner.getNext(qid)) != null) {
            Label sub= entityLabelHeapFile.getLabel(aquad.getSubjecqid().returnLID());
            Label obj= entityLabelHeapFile.getLabel(aquad.getObjecqid().returnLID());
            Label pred= predicateLabelHeapFile.getLabel(aquad.getPredicateID().returnLID());
            Double conf= aquad.getConfidence();

            if((SF.compareToIgnoreCase("*") == 0 || SF.compareTo(sub.getLabel()) == 0)
                    && (PF.compareToIgnoreCase("*") == 0 || PF.compareTo(pred.getLabel()) == 0)
                    && (OF.compareToIgnoreCase("*") == 0 || OF.compareTo(obj.getLabel()) == 0 && CF!=-1)
                    && ( CF ==-1 || CF <= conf)){
                EID[] nodes = new EID[]{aquad.getSubjecqid(), aquad.getObjecqid()};
                insertNewBasicPattern(heapfile, 2, nodes, aquad.getConfidence());
            }
        }
        return new BPFileScan(heapfile, 2);
    }

    public BPFileScan getJoinScan(BP_Triple_Join join, Heapfile heapfile) throws Exception {
        int num_nodes = join.LeftOutNodePosition.length + join.OutputRightSubject + join.OutputRightObject;
        BasicPattern bp;
        while ((bp = join.get_next()) != null) {
            EID[] nodes = bp.getNodeIDs();
            insertNewBasicPattern(heapfile, num_nodes, nodes, bp.getConfidence());
        }
        return new BPFileScan(heapfile, num_nodes);
    }

    public void printResult(BPFileScan result) throws Exception {
        BasicPattern bp;
        RID rid = new RID();
        while((bp = result.get_next())!=null)
        {
            bp.print();
        }
    }

    public void printResult(BPSort result) throws Exception {
        BasicPattern bp;
        RID rid = new RID();
        while((bp = result.get_next())!=null)
        {
            bp.print();
        }
    }

    public void executeQuery(int num_buf, String SF1, String PF1, String OF1, double CF1,
                             int JNP1, int JONO1, String RSF1, String RPF1, String ROF1, double RCF1, ArrayList<Integer> LONP1, int ORS1, int ORO1,
                             int JNP2, int JONO2, String RSF2, String RPF2, String ROF2, double RCF2, ArrayList<Integer> LONP2, int ORS2, int ORO2,
                             int SO, int SNP, int NP) throws Exception {

        // ToDo: Complete function. These are only stubs.

        // Input Heapfile
        Heapfile inputHF = new Heapfile(rdfDBname + "/inputHF");
        BPFileScan scanner = initBPScan(inputHF, SF1, PF1,OF1,CF1);
//        printResult(scanner);
//        System.out.println("Scan done");
        // Put Result of First Join in Heapfile
//        BP_Triple_Join join1 = new BP_Triple_Join(num_buf, 2, scanner,
//                JNP1, JONO1, RSF1, RPF1, ROF1, RCF1,
//                LONP1.stream().mapToInt(Integer::intValue).toArray(), ORS1, ORO1);
//        Heapfile join1hf = new Heapfile(rdfDBname + "/join1HF");
//        BPFileScan jscanner1 = getJoinScan(join1, join1hf);
//        printResult(jscanner1);

        // Put Result of Second Join in Heapfile
//        BP_Triple_Join join2 = new BP_Triple_Join(num_buf, 3, jscanner1,
//                JNP2, JONO2, RSF2, RPF2, ROF2, RCF2,
//                LONP2.stream().mapToInt(Integer::intValue).toArray(), ORS2, ORO2);
//        Heapfile join2hf = new Heapfile(rdfDBname + "/join2HF");
//        BPFileScan jscanner2 = getJoinScan(join2, join2hf);
//        jscanner1.close();
//        join1hf.deleteFile();

        // Stream Result of Sorted Result
        BPOrder order = new BPOrder(SO);
        BPSort result = new BPSort(scanner, SNP, order, NP);
//        jscanner2.close();
//
        printResult(result);

        scanner.close();
        inputHF.deleteFile();
        result.close();
//        join2hf.deleteFile();
    }


    public void closeRdfDBFiles() {

        try {
            entityBTree.close();

            predicateBTree.close();

            quadrupleBTree.close();

            distinctSubjectsBTree.close();

            distinctObjectsBTree.close();

        } catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

}
