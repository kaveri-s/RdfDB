package diskmgr;

import btree.*;
import global.*;
import labelheap.Label;
import labelheap.LabelHeapFile;
import quadrupleheap.Quadruple;
import quadrupleheap.QuadrupleHeapFile;
import quadrupleheap.TScan;

import java.util.HashMap;

public class rdfDB extends DB implements GlobalConst {
    private QuadrupleHeapFile quadrupleHeapFile;
    private LabelHeapFile entityLabelHeapFile;
    private LabelHeapFile predicateLabelHeapFile;
    private QuadrupleHeapFile tempQuadHeapFile;

    private LabelBTreeFile entityBTree;
    private LabelBTreeFile predicateBTree;
    private QuadBTreeFile quadrupleBTree;
    private QuadBTreeFile quadBTreeIndex;

    private LabelBTreeFile distinctSubjectsBTree;
    private LabelBTreeFile distinctObjectsBTree;

//    private PCounter pCounter;
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
//        pCounter = new PCounter();
//        pCounter.initialize();
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
        try{
            quadrupleBTree = new QuadBTreeFile(rdfDBname + "/quadBT");
        }catch(Exception e){
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
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

    public QuadBTreeFile getQuadBTreeIndex() {
        try{
            quadBTreeIndex = new QuadBTreeFile(rdfDBname + "/quadBTreeIndex");
        }catch(Exception e){
            System.err.println(e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return quadBTreeIndex;
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
            quadrupleBTree.close();
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
            entityBTree = new LabelBTreeFile(rdfDBname + "/entityBT");
            lid = insertLabel(entityLabel, entityBTree, entityLabelHeapFile);
            entityBTree.close();

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
            entityBTree = new LabelBTreeFile(rdfDBname + "/entityBT");
            result = deleteLabel(entityLabel, entityBTree, entityLabelHeapFile);
            entityBTree.close();
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
            predicateBTree = new LabelBTreeFile(rdfDBname + "/predicateBT");
            lid = insertLabel(predicateLabel, predicateBTree, predicateLabelHeapFile);
            predicateBTree.close();

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
            predicateBTree = new LabelBTreeFile(rdfDBname + "/predicateBT");
            result = deleteLabel(predicateLabel, predicateBTree, predicateLabelHeapFile);
            predicateBTree.close();
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
            quadrupleBTree = new QuadBTreeFile(rdfDBname + "/quadBT");
            String key = getKeyFromQuadPtr(quadruplePtr);
            double confidence = Convert.getFloValue(24, quadruplePtr);
            KeyClass low_key = new StringKey(key);
            KeyClass high_key = new StringKey(key);
            KeyDataEntry entry = null;

            QuadBTFileScan scan = quadrupleBTree.new_scan(low_key, high_key);
            entry = scan.get_next();

            if (entry != null && key.compareTo(((StringKey) (entry.key)).getKey()) == 0) {

                QID quadrupleID = ((QuadLeafData) (entry.data)).getData();
                Quadruple record = quadrupleHeapFile.getQuadruple(quadrupleID);
                double prevConfidence = record.getConfidence();
                if (prevConfidence < confidence) {
                    Quadruple newRecord = new Quadruple(quadruplePtr, 0);
                    quadrupleHeapFile.updateQuadruple(quadrupleID, newRecord);
                }
                scan.DestroyBTreeFileScan();
                quadrupleBTree.close();
                return quadrupleID;
            }

            qid = quadrupleHeapFile.insertQuadruple(quadruplePtr);
            quadrupleBTree.insert(low_key, qid);
            scan.DestroyBTreeFileScan();
            quadrupleBTree.close();

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
            quadrupleBTree = new QuadBTreeFile(rdfDBname + "/quadBT");
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
            quadrupleBTree.close();
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

    public void index_confidence() { createIndices(1); }

    public void index_subject() { createIndices(2); }

    public void index_subject_confidence() { createIndices(3); }

    public void index_object_confidence() { createIndices(5); }

    public void index_predicate_confidence() { createIndices(4); }

    private void createIndices(int indexType){
        try {
            removeExistingIndices();
            quadBTreeIndex = new QuadBTreeFile(rdfDBname + "/quadBTreeIndex", AttrType.attrString, 255, 1);
            TScan quadScan = new TScan(quadrupleHeapFile);
            Quadruple quad = null;
            QID qid = new QID();
            double confidence = 0.0;

            while ((quad = quadScan.getNext(qid)) != null) {
                KeyClass key = getKeyForIndex(quad, indexType);
                quadBTreeIndex.insert(key, qid);
            }
            quadScan.closescan();
            quadBTreeIndex.close();
        } catch (Exception e) {
            System.err.println("Creating index with confidence failed." + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

    private KeyClass getKeyForIndex(Quadruple quad, int indexType){
        KeyClass key = null;
        double confidence;
        Label subject;
        try{
            switch(indexType){
                case 1: // index on confidence
                    confidence = quad.getConfidence();
                    key = new StringKey(Double.toString(confidence));
                    break;

                case 2: // index on subject
                    subject = entityLabelHeapFile.getLabel(quad.getSubjecqid().returnLID());
                    key = new StringKey(subject.getLabel());
                    break;

                case 3: //index on subject_confidence
                    confidence = quad.getConfidence();
                    String temp = Double.toString(confidence);
                    subject = entityLabelHeapFile.getLabel(quad.getSubjecqid().returnLID());
                    key = new StringKey(subject.getLabel() + ":" + Double.toString(confidence));
                    break;

                case 4: //index on predicate_confidence
                    confidence = quad.getConfidence();
                    Label predicate = predicateLabelHeapFile.getLabel(quad.getPredicateID().returnLID());
                    key = new StringKey(predicate.getLabel() + ":" + Double.toString(confidence));
                    break;

                case 5: //index on object_confidence
                    confidence = quad.getConfidence();
                    Label object = entityLabelHeapFile.getLabel(quad.getObjecqid().returnLID());
                    key = new StringKey(object.getLabel() + ":" + Double.toString(confidence));
            }

        }catch(Exception e){
            System.err.println("Error while creating key for indexing. " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return key;

    }

    private void removeExistingIndices() {
        if (quadBTreeIndex != null) {

            try {
                quadBTreeIndex = new QuadBTreeFile(rdfDBname + "/quadBTreeIndex");
                QuadBTFileScan scan = quadBTreeIndex.new_scan(null, null);
                QID qid = null;
                KeyDataEntry entry = null;
                while ((entry = scan.get_next()) != null) {
                    qid = ((QuadLeafData) entry.data).getData();
                    quadBTreeIndex.Delete(entry.key, qid);
                }
                scan.DestroyBTreeFileScan();
                quadBTreeIndex.close();
                quadBTreeIndex.destroyFile();

            } catch (Exception e) {
                System.err.println("Deleting indexes failed " + e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
        }
    }

    private void initializeRdfDB() {
        try {
            quadrupleHeapFile = new QuadrupleHeapFile(rdfDBname + "/quadrupleHF");
            entityLabelHeapFile = new LabelHeapFile(rdfDBname + "/entityHF");
            predicateLabelHeapFile = new LabelHeapFile(rdfDBname + "/predicateHF");

            entityBTree = new LabelBTreeFile(rdfDBname + "/entityBT", AttrType.attrString, 255, 1);
            entityBTree.close();
            predicateBTree = new LabelBTreeFile(rdfDBname + "/predicateBT", AttrType.attrString, 255, 1);
            predicateBTree.close();
            quadrupleBTree = new QuadBTreeFile(rdfDBname + "/quadBT", AttrType.attrString, 255, 1);
            quadrupleBTree.close();
            distinctSubjectsBTree = new LabelBTreeFile(rdfDBname + "/distinctSubjBT", AttrType.attrString, 255, 1);
            distinctSubjectsBTree.close();
            distinctObjectsBTree = new LabelBTreeFile(rdfDBname + "/distinctObjBT", AttrType.attrString, 255, 1);
            distinctObjectsBTree.close();
            quadBTreeIndex = new QuadBTreeFile(rdfDBname + "/quadBTreeIndex", AttrType.attrString, 255, 1);
            quadBTreeIndex.close();

        } catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

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


    public void closeRdfDBFiles() {

        try {
            if (entityBTree != null) entityBTree.close();

            if (predicateBTree != null) predicateBTree.close();

            if (quadrupleBTree != null) quadrupleBTree.close();

            if (distinctSubjectsBTree != null) distinctSubjectsBTree.close();

            if (distinctObjectsBTree != null) distinctObjectsBTree.close();

            if (quadBTreeIndex != null) quadBTreeIndex.close();

//            super.closeDB();
        } catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

}
