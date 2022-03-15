package iterator;

import global.*;
import labelheap.Label;
import quadrupleheap.*;

import java.io.IOException;

public class QuadrupleUtils {
    public static int CompareQuadrupleWithQuadruple(
            Quadruple q1,
            Quadruple  q2, int quadruple_fld_no)
    {

        switch (quadruple_fld_no)
        {
            case 1:                // Compare subject
                EID s1=q1.getSubjecqid();
                EID s2=q2.getSubjecqid();
                LID sl1=s1.returnLID();
                LID sl2=s2.returnLID();
                if(sl1==sl2) return 0;
                else{

                }
                //get label from ID and compare

            case 2:                // Compare predicate
                PID p1=q1.getPredicateID();
                PID p2=q2.getPredicateID();
                LID pl1=p1.returnLID();
                LID pl2=p2.returnLID();
                if(pl1==pl2) return 0;
                else{

                }
                //get label from ID and compare
            case 3:                // Compare object
                EID o1=q1.getObjecqid();
                EID o2=q2.getObjecqid();
                LID ol1=o1.returnLID();
                LID ol2=o2.returnLID();
                if(ol1==ol2) return 0;
                else{

                }
                //get label from ID and compare
            case 4:                // Compare confidence
                double c1= q1.getConfidence();
                double c2= q2.getConfidence();
                if(c1>c2) return 1;
                if(c2<c1) return -1;
                return 0;
        }
    }
    public static boolean Equal(Quadruple q1, Quadruple q2)
    {
        int i;
        for (i = 1; i <= 4; i++)
            if (CompareQuadrupleWithQuadruple(q1,q2,i) != 0)
                return false;
        return true;
    }
}
