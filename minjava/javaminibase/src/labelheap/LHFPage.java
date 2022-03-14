package labelheap;

import diskmgr.Page;
import global.Convert;
import global.LID;
import global.PageId;
import heap.HFPage;
import heap.InvalidSlotNumberException;

import java.io.IOException;

public class LHFPage extends HFPage {
    public LHFPage() {
        super();
    }

    /**
     * Constructor of class THFPage
     * open a THFPage and make this HFpage piont to the given page
     * @param  page  the given page in Page type
     */

    public LHFPage(Page page)
    {
        super(page);
    }

    /**
     * @return byte array
     */

    public byte [] getTHFpageArray()
    {
        return data;
    }

    /**
     * inserts a new record onto the page, returns RID of this record
     * @param	record 	a record to be inserted
     * @return	RID of record, null if sufficient space does not exist
     * @exception IOException I/O errors
     * in C++ Status insertRecord(char *recPtr, int recLen, RID& rid)
     */
    @Override
    public LID insertRecord (byte [] record)
            throws IOException
    {
        LID lid = new LID();

        int recLen = record.length;
        int spaceNeeded = recLen + SIZE_OF_SLOT;

        // Start by checking if sufficient space exists.
        // This is an upper bound check. May not actually need a slot
        // if we can find an empty one.

        freeSpace = Convert.getShortValue (FREE_SPACE, data);
        if (spaceNeeded > freeSpace) {
            return null;

        } else {

            // look for an empty slot
            slotCnt = Convert.getShortValue (SLOT_CNT, data);
            int i;
            short length;
            for (i= 0; i < slotCnt; i++)
            {
                length = getSlotLength(i);
                if (length == EMPTY_SLOT)
                    break;
            }

            if(i == slotCnt)   //use a new slot
            {
                // adjust free space
                freeSpace -= spaceNeeded;
                Convert.setShortValue (freeSpace, FREE_SPACE, data);

                slotCnt++;
                Convert.setShortValue (slotCnt, SLOT_CNT, data);

            }
            else {
                // reusing an existing slot
                freeSpace -= recLen;
                Convert.setShortValue (freeSpace, FREE_SPACE, data);
            }

            usedPtr = Convert.getShortValue (USED_PTR, data);
            usedPtr -= recLen;    // adjust usedPtr
            Convert.setShortValue (usedPtr, USED_PTR, data);

            //insert the slot info onto the data page
            setSlot(i, recLen, usedPtr);

            // insert data onto the data page
            System.arraycopy (record, 0, data, usedPtr, recLen);
            curPage.pid = Convert.getIntValue (CUR_PAGE, data);
            lid.pageNo.pid = curPage.pid;
            lid.slotNo = i;
            return   lid;
        }
    }

    /**
     * delete the record with the specified rid
     * @param	qid 	the record ID
     * @exception InvalidSlotNumberException Invalid slot number
     * @exception IOException I/O errors
     * in C++ Status deleteRecord(const RID& rid)
     */
    @Override
    public void deleteRecord( LID lid )
            throws IOException,
            InvalidSlotNumberException
    {
        int slotNo = lid.slotNo;
        short recLen = getSlotLength (slotNo);
        slotCnt = Convert.getShortValue (SLOT_CNT, data);

        // first check if the record being deleted is actually valid
        if ((slotNo >= 0) && (slotNo < slotCnt) && (recLen > 0))
        {
            // The records always need to be compacted, as they are
            // not necessarily stored on the page in the order that
            // they are listed in the slot index.

            // offset of record being deleted
            int offset = getSlotOffset(slotNo);
            usedPtr = Convert.getShortValue (USED_PTR, data);
            int newSpot= usedPtr + recLen;
            int size = offset - usedPtr;

            // shift bytes to the right
            System.arraycopy(data, usedPtr, data, newSpot, size);

            // now need to adjust offsets of all valid slots that refer
            // to the left of the record being removed. (by the size of the hole)

            int i, n, chkoffset;
            for (i = 0, n = DPFIXED; i < slotCnt; n +=SIZE_OF_SLOT, i++) {
                if ((getSlotLength(i) >= 0))
                {
                    chkoffset = getSlotOffset(i);
                    if(chkoffset < offset)
                    {
                        chkoffset += recLen;
                        Convert.setShortValue((short)chkoffset, n+2, data);
                    }
                }
            }

            // move used Ptr forwar
            usedPtr += recLen;
            Convert.setShortValue (usedPtr, USED_PTR, data);

            // increase freespace by size of hole
            freeSpace = Convert.getShortValue(FREE_SPACE, data);
            freeSpace += recLen;
            Convert.setShortValue (freeSpace, FREE_SPACE, data);

            setSlot(slotNo, EMPTY_SLOT, 0);  // mark slot free
        }
        else {
            throw new InvalidSlotNumberException (null, "HEAPFILE: INVALID_SLOTNO");
        }
    }

    /**
     * @return QID of first record on page, null if page contains no records.
     * @exception  IOException I/O errors
     * in C++ Status firstRecord(RID& firstRid)
     *
     */
    @Override
    public LID firstRecord()
            throws IOException
    {
        LID lid = new LID();
        // find the first non-empty slot

        slotCnt = Convert.getShortValue (SLOT_CNT, data);

        int i;
        short length;
        for (i= 0; i < slotCnt; i++)
        {
            length = getSlotLength (i);
            if (length != EMPTY_SLOT)
                break;
        }

        if(i== slotCnt)
            return null;

        // found a non-empty slot

        lid.slotNo = i;
        curPage.pid= Convert.getIntValue(CUR_PAGE, data);
        lid.pageNo.pid = curPage.pid;

        return lid;
    }

    /**
     * @return QID of next record on the page, null if no more
     * records exist on the page
     * @param 	curLid	current record ID
     * @exception  IOException I/O errors
     * in C++ Status nextRecord (RID curRid, RID& nextRid)
     */
    public LID nextRecord (LID curLid)
            throws IOException
    {
        LID lid = new LID();
        slotCnt = Convert.getShortValue (SLOT_CNT, data);

        int i=curLid.slotNo;
        short length;

        // find the next non-empty slot
        for (i++; i < slotCnt;  i++)
        {
            length = getSlotLength(i);
            if (length != EMPTY_SLOT)
                break;
        }

        if(i >= slotCnt)
            return null;

        // found a non-empty slot

        lid.slotNo = i;
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        lid.pageNo.pid = curPage.pid;

        return lid;
    }

    /**
     * copies out record with QID qid into record pointer.
     * <br>
     * Status getRecord(QID qid, char *recPtr, int& recLen)
     * @param	lid 	the record ID
     * @return 	a tuple contains the record
     * @exception   InvalidSlotNumberException Invalid slot number
     * @exception  	IOException I/O errors
     * @see    Label
     */
    public Label getRecord ( LID lid )
            throws IOException,
            InvalidSlotNumberException
    {
        short recLen;
        short offset;
        byte []record;
        PageId pageNo = new PageId();
        pageNo.pid= lid.pageNo.pid;
        curPage.pid = Convert.getIntValue (CUR_PAGE, data);
        int slotNo = lid.slotNo;

        // length of record being returned
        recLen = getSlotLength (slotNo);
        slotCnt = Convert.getShortValue (SLOT_CNT, data);
        if (( slotNo >=0) && (slotNo < slotCnt) && (recLen >0)
                && (pageNo.pid == curPage.pid))
        {
            offset = getSlotOffset (slotNo);
            record = new byte[recLen];
            System.arraycopy(data, offset, record, 0, recLen);
            Label label = new Label(record, recLen);
            return label;
        }

        else {
            throw new InvalidSlotNumberException (null, "HEAPFILE: INVALID_SLOTNO");
        }


    }

    /**
     * returns a tuple in a byte array[pageSize] with given RID rid.
     * <br>
     * in C++	Status returnRecord(RID rid, char*& recPtr, int& recLen)
     * @param       lid     the record ID
     * @return      a tuple  with its length and offset in the byte array
     * @exception   InvalidSlotNumberException Invalid slot number
     * @exception   IOException I/O errors
     * @see    Label
     */
    public Label returnRecord ( LID lid )
            throws IOException,
            InvalidSlotNumberException
    {
        short recLen;
        short offset;
        PageId pageNo = new PageId();
        pageNo.pid = lid.pageNo.pid;

        curPage.pid = Convert.getIntValue (CUR_PAGE, data);
        int slotNo = lid.slotNo;

        // length of record being returned
        recLen = getSlotLength (slotNo);
        slotCnt = Convert.getShortValue (SLOT_CNT, data);

        if (( slotNo >=0) && (slotNo < slotCnt) && (recLen >0)
                && (pageNo.pid == curPage.pid))
        {

            offset = getSlotOffset (slotNo);
            Label quadruple = new Label(data, recLen);
            return quadruple;
        }

        else {
            throw new InvalidSlotNumberException (null, "HEAPFILE: INVALID_SLOTNO");
        }
    }
}
