package diskmgr;

import btree.*;
import global.*;
import labelheap.Label;
import labelheap.LabelHeapFile;
import quadrupleheap.Quadruple;
import quadrupleheap.QuadrupleHeapFile;
import quadrupleheap.TScan;

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

    private PCounter pCounter;
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
        pCounter = new PCounter();
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
            pCounter.readIncrement();
        }catch(Exception e){
            System.err.println("Error while reading the page from the disk " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

    public void write_page(PageId pageno, Page apage){
        try{
            super.write_page(pageno, apage);
            pCounter.writeIncrement();
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

    public QuadBTreeFile getQuadBTreeIndex() {
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

        try {
            QuadBTFileScan scan = quadrupleBTree.new_scan(null, null);
            entry = scan.get_next();

            //scan the quadBTTree and insert the new distinct values to the distinctSubjectBT
            while (entry != null) ;
            {
                String label = ((StringKey) (entry.key)).getKey();
                String[] temp;
                String delimiter = ":";
                temp = label.split(delimiter);
                String subject = temp[0] + temp[1];
                KeyClass low_key = new StringKey(subject);
                KeyClass high_key = new StringKey(subject);
                LabelBTFileScan distinctSubjectScan = distinctSubjectsBTree.new_scan(low_key, high_key);
                KeyDataEntry dup_entry = distinctSubjectScan.get_next();
                if (dup_entry == null) {
                    //subject not present in btree, hence insert
                    distinctSubjectsBTree.insert(low_key, new LID(new PageId(Integer.parseInt(temp[1])), Integer.parseInt(temp[0])));
                }
                distinctSubjectScan.DestroyBTreeFileScan();
                entry = scan.get_next();
            }
            scan.DestroyBTreeFileScan();
            LabelBTFileScan distinctSubjectBTScan = distinctSubjectsBTree.new_scan(null, null);
            entry = distinctSubjectBTScan.get_next();
            while (entry != null) {
                subjectsCount++;
                entry = distinctSubjectBTScan.get_next();
            }
            ;
            distinctSubjectBTScan.DestroyBTreeFileScan();

        } catch (Exception e) {
            System.err.println("Error while fetching the quadruples count. " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return subjectsCount;
    }

    public int getObjectCnt() {
        objectsCount = 0;
        KeyDataEntry entry = null;

        try {
            QuadBTFileScan scan = quadrupleBTree.new_scan(null, null);
            entry = scan.get_next();

            //scan the quadBTTree and insert the new distinct values to the distinctSubjectBT
            while (entry != null) ;
            {
                String label = ((StringKey) (entry.key)).getKey();
                String[] temp;
                String delimiter = ":";
                temp = label.split(delimiter);
                String object = temp[4] + temp[5];
                KeyClass low_key = new StringKey(object);
                KeyClass high_key = new StringKey(object);
                LabelBTFileScan distinctObjectScan = distinctObjectsBTree.new_scan(low_key, high_key);
                KeyDataEntry dup_entry = distinctObjectScan.get_next();
                if (dup_entry == null) {
                    //subject not present in btree, hence insert
                    distinctObjectsBTree.insert(low_key, new LID(new PageId(Integer.parseInt(temp[1])), Integer.parseInt(temp[0])));
                }
                distinctObjectScan.DestroyBTreeFileScan();
                entry = scan.get_next();
            }
            scan.DestroyBTreeFileScan();
            LabelBTFileScan distinctObjectBTScan = distinctSubjectsBTree.new_scan(null, null);
            entry = distinctObjectBTScan.get_next();
            while (entry != null) {
                objectsCount++;
                entry = distinctObjectBTScan.get_next();
            }
            ;
            distinctObjectBTScan.DestroyBTreeFileScan();

        } catch (Exception e) {
            System.err.println("Error while fetching the quadruples count. " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return objectsCount;
    }

    public EID insertEntity(String entityLabel) {
        LID lid = insertLabel(entityLabel, entityBTree, entityLabelHeapFile);
        return lid.returnEID();
    }

    public boolean deleteEntity(String entityLabel) {
        return deleteLabel(entityLabel, entityBTree, entityLabelHeapFile);
    }

    public PID insertPredicate(String predicateLabel) {
        LID lid = insertLabel(predicateLabel, predicateBTree, predicateLabelHeapFile);
        return lid.returnPID();
    }

    public boolean deletePredicate(String predicateLabel) {
        return deleteLabel(predicateLabel, predicateBTree, predicateLabelHeapFile);
    }

    public QID insertQuadruple(byte[] quadruplePtr) {
        QID qid = null;

        try {
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
                return quadrupleID;
            }

            qid = quadrupleHeapFile.insertQuadruple(quadruplePtr);
            quadrupleBTree.insert(low_key, qid);
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
            if (entry != null && key.compareTo(((StringKey) (entry.key)).getKey()) == 0) {
                QID quadrupleId = ((QuadLeafData) (entry.data)).getData();
                if (quadrupleId != null)
                    isDeleteSuccessful = quadrupleBTree.Delete(low_key, quadrupleId) && quadrupleHeapFile.deleteQuadruple(quadrupleId);

            }
            scan.DestroyBTreeFileScan();

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

    public void insertNewQuadruple(String data[]) {
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
            insertTempQuadruple(quad.getQuadrupleByteArray());
        } catch (Exception e) {
            System.err.println("Insert temp Quadruple failed.");
            e.printStackTrace();
        }
    }

    public void sortAndInsertQuadruples(boolean dbexists, int indexOption) {
        QuadrupleOrder order = getSortOrder(indexOption);
        Quadruple aquad = null;
        try {
            if (dbexists) {

                TScan tScanner = new TScan(getQuadrupleHandle());
                QID qid = new QID();

                while ((aquad = tScanner.getNext(qid)) != null) {
                    insertTempQuadruple(aquad.getQuadrupleByteArray());
                    deleteQuadruple(aquad.getQuadrupleByteArray());
                }
            }
            TScan tScanner = new TScan(tempQuadHeapFile);

            QuadrupleSort qSort = new QuadrupleSort(tScanner, order, 200);

            while ((aquad = qSort.getNext()) != null) {
                insertQuadruple(aquad.getQuadrupleByteArray());
            }
            tempQuadHeapFile.deleteFile();
            tempQuadHeapFile = null;
            tScanner.closescan();
        } catch (Exception e) {
            System.err.println("sort and insert Quadruple failed.");
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

            while ((quad = quadScan.getNext(null)) != null) {
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
                {
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
                }
            } catch (Exception e) {
                System.err.println("Deleting indexes failed " + e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
            }
        }
    }

    private void insertTempQuadruple(byte[] quad) throws Exception {
        try {
            if (tempQuadHeapFile == null)
                tempQuadHeapFile = new QuadrupleHeapFile(rdfDBname + "/tempQuadHeapFile");
            QID qid = tempQuadHeapFile.insertQuadruple(quad);
//            System.out.println("Successfully inserted Quadruple at " + qid.pageNo + ", " + qid.slotNo);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initializeRdfDB() {

        pCounter.initialize();
        try {
            quadrupleHeapFile = new QuadrupleHeapFile(rdfDBname + "/quadrupleHF");
            entityLabelHeapFile = new LabelHeapFile(rdfDBname + "/entityHF");
            predicateLabelHeapFile = new LabelHeapFile(rdfDBname + "/predicateHF");

            entityBTree = new LabelBTreeFile(rdfDBname + "/entityBT", AttrType.attrString, 255, 1);
            predicateBTree = new LabelBTreeFile(rdfDBname + "/predicateBT", AttrType.attrString, 255, 1);
            quadrupleBTree = new QuadBTreeFile(rdfDBname + "/quadBT", AttrType.attrString, 255, 1);
            distinctSubjectsBTree = new LabelBTreeFile(rdfDBname + "/distinctSubjBT");
            distinctObjectsBTree = new LabelBTreeFile(rdfDBname + "/distinctObjBT");
            quadBTreeIndex = new QuadBTreeFile(rdfDBname + "/quadBTreeIndex", AttrType.attrString, 255, 1);

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
            Runtime.getRuntime().exit(1);
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

    private static QuadrupleOrder getSortOrder(int indexOption)
    {
        switch(indexOption)
        {
            case 1:
                return new QuadrupleOrder(QuadrupleOrder.SubjectConfidence);
            case 2:
                return new QuadrupleOrder(QuadrupleOrder.PredicateConfidence);
            case 3:
                return new QuadrupleOrder(QuadrupleOrder.ObjectConfidence);
            case 4:
                return new QuadrupleOrder(QuadrupleOrder.Confidence);
            case 5:
                return new QuadrupleOrder(QuadrupleOrder.Subject);
            default:
                System.err.println("RuntimeError. Sort order out of range (1-5). Welp, shouldn't be here");
        }
        return null;
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


    public void closeDB() {

        try {
            if (entityBTree != null) entityBTree.close();

            if (predicateBTree != null) predicateBTree.close();

            if (quadrupleBTree != null) quadrupleBTree.close();

            if (distinctSubjectsBTree != null) distinctSubjectsBTree.close();

            if (distinctObjectsBTree != null) distinctObjectsBTree.close();

            if (quadBTreeIndex != null) quadBTreeIndex.close();

            super.closeDB();
        } catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

}
