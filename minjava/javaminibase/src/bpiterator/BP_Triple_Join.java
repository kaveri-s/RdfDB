package bpiterator;

import basicpattern.BasicPattern;
import bufmgr.PageNotReadException;
import diskmgr.Stream;
import global.EID;
import global.QID;
import global.SystemDefs;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import index.IndexException;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.PredEvalException;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;
import labelheap.Label;
import labelheap.LabelHeapFile;
import quadrupleheap.Quadruple;

import java.io.IOException;
import java.util.ArrayList;

//TODO: Change it to BPIterator
public class BP_Triple_Join extends BPIterator {

    int amt_of_mem;
    int num_left_nodes;
    BPFileScan left_itr;
    int BPJoinNodePosition;
    int JoinOnSubjectorObject;
    String RightSubjectFilter;
    String RightPredicateFilter;
    String RightObjectFilter;
    double RightConfidenceFilter;
    public int [] LeftOutNodePosition;
    public int OutputRightSubject;
    public int OutputRightObject;

    private boolean done;
    private boolean getFromOuter;
    private Stream innerStream;
    private BasicPattern outer_bp;
    private Quadruple inner_qr;
    private boolean useIndex;
    private int outerCount;
    private int innerCount;

    public BP_Triple_Join( int amt_of_mem,
                    int num_left_nodes,
                    BPFileScan left_itr,
                    int BPJoinNodePosition,
                    int JoinOnSubjectorObject,
                    String RightSubjectFilter,
                    String RightPredicateFilter,
                    String RightObjectFilter,
                    double RightConfidenceFilter,
                    int [] LeftOutNodePositions,
                    int OutputRightSubject,
                    int OutputRightObject,
                    boolean useIndex){

        this.amt_of_mem = amt_of_mem;
        this.num_left_nodes = num_left_nodes;
        this.left_itr = left_itr;
        this.BPJoinNodePosition = BPJoinNodePosition;
        this.JoinOnSubjectorObject = JoinOnSubjectorObject;
        this.RightSubjectFilter = new String(RightSubjectFilter);
        this.RightObjectFilter = new String(RightObjectFilter);
        this.RightPredicateFilter = new String(RightPredicateFilter);
        this.RightConfidenceFilter = RightConfidenceFilter;
        this.LeftOutNodePosition = LeftOutNodePositions;
        this.OutputRightSubject = OutputRightSubject;
        this.OutputRightObject = OutputRightObject;

        this.getFromOuter = true;
        this.innerStream = null;
        this.outer_bp = null;
        this.inner_qr = null;
        this.done = false;
        this.useIndex = useIndex;
        this.outerCount = 0;
        this.innerCount = 0;
    }

    public int getInnerCount(){ return this.innerCount;}

    public int getOuterCount(){ return this.outerCount;}

    //TODO: Change Tuple to BasicPattern
    @Override
    public BasicPattern get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        //if the scan is done, return null
        if (done) return null;

        do {

            if (getFromOuter == true) {
                getFromOuter = false;
                if (innerStream != null) {
                    innerStream.closeStream();
                    innerStream = null;
                }

                try {
                    innerStream = SystemDefs.JavabaseDB.openStream(RightSubjectFilter, RightPredicateFilter, RightObjectFilter, RightConfidenceFilter, this.amt_of_mem, useIndex);
                } catch (Exception e) {
                    System.out.println("Open Stream failed during Triple Join: "+ e);
                }
                //if the scan on basicPattern is complete, return null
                if ((outer_bp = left_itr.get_next()) == null) {
                    done = true;
                    if (innerStream != null) {
                        innerStream.closeStream();
                        innerStream = null;
                    }
                    return null;
                }
                this.outerCount++;
            }

            //Fetch the inner quadruple
            QID qid = new QID();


            while ((inner_qr = innerStream.getNext(qid)) != null) {

                if (compareFilters() == true) {
                    this.innerCount++;
                    double confidence = inner_qr.getConfidence();
                    ArrayList<EID> EIDs = new ArrayList<EID>();
                    EID outerEID = outer_bp.getNodeID(BPJoinNodePosition);
                    EID innerEID = JoinOnSubjectorObject == 0 ? inner_qr.getSubjecqid() : inner_qr.getObjecqid();

                    double min_conf = Math.min(confidence, outer_bp.getConfidence());

                    if (outerEID.equals(innerEID)) {
                        for (int j = 0; j < LeftOutNodePosition.length; j++) {
                            EIDs.add(outer_bp.getNodeID(LeftOutNodePosition[j]));
                        }

                        if (OutputRightSubject == 1) {
                            EIDs.add(inner_qr.getSubjecqid());
                        }

                        if (OutputRightObject == 1) {
                            EIDs.add(inner_qr.getObjecqid());
                        }

                        if (EIDs.size() != 0) {
                            BasicPattern bp = new BasicPattern((short) (EIDs.size()));
                            for (int k = 0; k < EIDs.size(); k++) {
                                bp.setNodeID(k, EIDs.get(k));
                            }
                            bp.setConfidence(min_conf);
                            return bp;
                        }
                    }
                }
            }
            getFromOuter = true;
        }while(true);
    }

    private boolean compareFilters() throws InvalidTupleSizeException, Exception{
        LabelHeapFile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapFile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();
        double confidence = 0;
        confidence = inner_qr.getConfidence();
        Label subject = Entity_HF.getLabel(inner_qr.getSubjecqid().returnLID());
        Label predicate = Predicate_HF.getLabel(inner_qr.getPredicateID().returnLID());
        Label object = Entity_HF.getLabel(inner_qr.getObjecqid().returnLID());
        boolean result = true;

        if(RightSubjectFilter.compareToIgnoreCase("*") != 0)
        {
            result = result & (RightSubjectFilter.compareTo(subject.getLabel()) == 0);
        }
        if(RightObjectFilter.compareToIgnoreCase("*") != 0)
        {
            result = result & (RightObjectFilter.compareTo(object.getLabel()) == 0 );
        }
        if(RightPredicateFilter.compareToIgnoreCase("*") != 0)
        {
            result = result & (RightPredicateFilter.compareTo(predicate.getLabel()) == 0 );
        }
        if(RightConfidenceFilter != 0)
        {
            result = result & (confidence >= RightConfidenceFilter);
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        if (!closeFlag) {
            try {
                if(innerStream!=null) innerStream.closeStream();
                left_itr.close();
            }catch (Exception e) {
                System.out.println("Error in closing triple join iterator."+e);
            }
            closeFlag = true;
        }
    }
}
