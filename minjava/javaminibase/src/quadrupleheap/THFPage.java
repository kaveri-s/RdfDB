/* File THFPage.java */

package quadrupleheap;

import java.io.*;
import java.lang.*;

import global.*;
import diskmgr.*;
import heap.*;

import static heap.HFPage.*;

/** Class quadrupleheap file page.
 * The design assumes that records are kept compacted when
 * deletions are performed. 
 */

public class THFPage extends Page implements GlobalConst{

    /**
     * number of slots in use
     */
    private    short     slotCnt;

    /**
     * offset of first used byte by data records in data[]
     */
    private    short     usedPtr;

    /**
     * number of bytes free in data[]
     */
    private    short     freeSpace;

    /**
     * an arbitrary value used by subclasses as needed
     */
    private    short     type;

    /**
     * backward pointer to data page
     */
    private    PageId   prevPage = new PageId();

    /**
     * forward pointer to data page
     */
    private   PageId    nextPage = new PageId();

    /**
     *  page number of this page
     */
    private    PageId    curPage = new PageId();


    public THFPage() {
    }

    /**
     * Constructor of class THFPage
     * open a THFPage and make this HFpage piont to the given page
     * @param  page  the given page in Page type
     */

    public THFPage(Page page)
    {
        data = page.getpage();
    }

    public void openHFpage(Page apage)
    {
        data = apage.getpage();
    }

    /**
     * Constructor of class HFPage
     * initialize a new page
     * @param	pageNo	the page number of a new page to be initialized
     * @param	apage	the Page to be initialized
     * @see		Page
     * @exception IOException I/O errors
     */

    public void init(PageId pageNo, Page apage)
            throws IOException
    {
        data = apage.getpage();

        slotCnt = 0;                // no slots in use
        Convert.setShortValue (slotCnt, SLOT_CNT, data);

        curPage.pid = pageNo.pid;
        Convert.setIntValue (curPage.pid, CUR_PAGE, data);

        nextPage.pid = prevPage.pid = INVALID_PAGE;
        Convert.setIntValue (prevPage.pid, PREV_PAGE, data);
        Convert.setIntValue (nextPage.pid, NEXT_PAGE, data);

        usedPtr = (short) MAX_SPACE;  // offset in data array (grow backwards)
        Convert.setShortValue (usedPtr, USED_PTR, data);

        freeSpace = (short) (MAX_SPACE - DPFIXED);    // amount of space available
        Convert.setShortValue (freeSpace, FREE_SPACE, data);

    }

    /**
     * @return byte array
     */

    public byte [] getTHFpageArray()
    {
        return data;
    }


    /**
     * @return	PageId of previous page
     * @exception IOException I/O errors
     */
    public PageId getPrevPage()
            throws IOException
    {
        prevPage.pid =  Convert.getIntValue (PREV_PAGE, data);
        return prevPage;
    }

    /**
     * sets value of prevPage to pageNo
     * @param       pageNo  page number for previous page
     * @exception IOException I/O errors
     */
    public void setPrevPage(PageId pageNo)
            throws IOException
    {
        prevPage.pid = pageNo.pid;
        Convert.setIntValue (prevPage.pid, PREV_PAGE, data);
    }

    /**
     * @return     page number of next page
     * @exception IOException I/O errors
     */
    public PageId getNextPage()
            throws IOException
    {
        nextPage.pid =  Convert.getIntValue (NEXT_PAGE, data);
        return nextPage;
    }

    /**
     * sets value of nextPage to pageNo
     * @param	pageNo	page number for next page
     * @exception IOException I/O errors
     */
    public void setNextPage(PageId pageNo)
            throws IOException
    {
        nextPage.pid = pageNo.pid;
        Convert.setIntValue (nextPage.pid, NEXT_PAGE, data);
    }

    /**
     * @return 	page number of current page
     * @exception IOException I/O errors
     */
    public PageId getCurPage()
            throws IOException
    {
        curPage.pid =  Convert.getIntValue (CUR_PAGE, data);
        return curPage;
    }

    /**
     * sets value of curPage to pageNo
     * @param	pageNo	page number for current page
     * @exception IOException I/O errors
     */
    public void setCurPage(PageId pageNo)
            throws IOException
    {
        curPage.pid = pageNo.pid;
        Convert.setIntValue (curPage.pid, CUR_PAGE, data);
    }

    /**
     * @return 	the ype
     * @exception IOException I/O errors
     */
    public short getType()
            throws IOException
    {
        type =  Convert.getShortValue (TYPE, data);
        return type;
    }

    /**
     * sets value of type
     * @param	valtype     an arbitrary value
     * @exception IOException I/O errors
     */
    public void setType(short valtype)
            throws IOException
    {
        type = valtype;
        Convert.setShortValue (type, TYPE, data);
    }

    /**
     * @return 	slotCnt used in this page
     * @exception IOException I/O errors
     */
    public short getSlotCnt()
            throws IOException
    {
        slotCnt =  Convert.getShortValue (SLOT_CNT, data);
        return slotCnt;
    }

    /**
     * sets slot contents
     * @param       slotno  the slot number
     * @param 	length  length of record the slot contains
     * @param	offset  offset of record
     * @exception IOException I/O errors
     */
    public void setSlot(int slotno, int length, int offset)
            throws IOException
    {
        int position = DPFIXED + slotno * SIZE_OF_SLOT;
        Convert.setShortValue((short)length, position, data);
        Convert.setShortValue((short)offset, position+2, data);
    }

    /**
     * @param	slotno	slot number
     * @exception IOException I/O errors
     * @return	the length of record the given slot contains
     */
    public short getSlotLength(int slotno)
            throws IOException
    {
        int position = DPFIXED + slotno * SIZE_OF_SLOT;
        short val= Convert.getShortValue(position, data);
        return val;
    }

    /**
     * @param       slotno  slot number
     * @exception IOException I/O errors
     * @return      the offset of record the given slot contains
     */
    public short getSlotOffset(int slotno)
            throws IOException
    {
        int position = DPFIXED + slotno * SIZE_OF_SLOT;
        short val= Convert.getShortValue(position +2, data);
        return val;
    }

    /**
     * inserts a new record onto the page, returns RID of this record
     * @param	record 	a record to be inserted
     * @return	RID of record, null if sufficient space does not exist
     * @exception IOException I/O errors
     * in C++ Status insertRecord(char *recPtr, int recLen, RID& rid)
     */
    public QID insertRecord ( byte [] record)
            throws IOException
    {
        QID qid = new QID();

        int recLen = record.length;
        int spaceNeeded = recLen + SIZE_OF_SLOT;

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
            qid.pageNo.pid = curPage.pid;
            qid.slotNo = i;
            return   qid ;
        }
    }

    /**
     * delete the record with the specified rid
     * @param	qid 	the record ID
     * @exception	InvalidSlotNumberException Invalid slot number
     * @exception IOException I/O errors
     * in C++ Status deleteRecord(const RID& rid)
     */
    public void deleteRecord( QID qid )
            throws IOException,
            InvalidSlotNumberException
    {
        int slotNo = qid.slotNo;
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
    public QID firstRecord()
            throws IOException
    {
        QID qid = new QID();
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

        qid.slotNo = i;
        curPage.pid= Convert.getIntValue(CUR_PAGE, data);
        qid.pageNo.pid = curPage.pid;

        return qid;
    }

    /**
     * @return QID of next record on the page, null if no more
     * records exist on the page
     * @param 	curQid	current record ID
     * @exception  IOException I/O errors
     * in C++ Status nextRecord (RID curRid, RID& nextRid)
     */
    public QID nextRecord (QID curQid)
            throws IOException
    {
        QID qid = new QID();
        slotCnt = Convert.getShortValue (SLOT_CNT, data);

        int i=curQid.slotNo;
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

        qid.slotNo = i;
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        qid.pageNo.pid = curPage.pid;

        return qid;
    }

    /**
     * copies out record with QID qid into record pointer.
     * <br>
     * Status getRecord(QID qid, char *recPtr, int& recLen)
     * @param	qid 	the record ID
     * @return 	a tuple contains the record
     * @exception   InvalidSlotNumberException Invalid slot number
     * @exception  	IOException I/O errors
     * @see    Quadruple
     */
    public Quadruple getRecord ( QID qid )
            throws IOException,
            InvalidSlotNumberException
    {
        short recLen;
        short offset;
        byte []record;
        PageId pageNo = new PageId();
        pageNo.pid= qid.pageNo.pid;
        curPage.pid = Convert.getIntValue (CUR_PAGE, data);
        int slotNo = qid.slotNo;

        // length of record being returned
        recLen = getSlotLength (slotNo);
        slotCnt = Convert.getShortValue (SLOT_CNT, data);
        if (( slotNo >=0) && (slotNo < slotCnt) && (recLen >0)
                && (pageNo.pid == curPage.pid))
        {
            offset = getSlotOffset (slotNo);
            record = new byte[recLen];
            System.arraycopy(data, offset, record, 0, recLen);
            Quadruple quadruple = new Quadruple(record, recLen);
            return quadruple;
        }

        else {
            throw new InvalidSlotNumberException (null, "HEAPFILE: INVALID_SLOTNO");
        }


    }

    /**
     * returns a tuple in a byte array[pageSize] with given RID rid.
     * <br>
     * in C++	Status returnRecord(RID rid, char*& recPtr, int& recLen)
     * @param       qid     the record ID
     * @return      a tuple  with its length and offset in the byte array
     * @exception   InvalidSlotNumberException Invalid slot number
     * @exception   IOException I/O errors
     * @see    Quadruple
     */
    public Quadruple returnRecord ( QID qid )
            throws IOException,
            InvalidSlotNumberException
    {
        short recLen;
        short offset;
        PageId pageNo = new PageId();
        pageNo.pid = qid.pageNo.pid;

        curPage.pid = Convert.getIntValue (CUR_PAGE, data);
        int slotNo = qid.slotNo;

        // length of record being returned
        recLen = getSlotLength (slotNo);
        slotCnt = Convert.getShortValue (SLOT_CNT, data);

        if (( slotNo >=0) && (slotNo < slotCnt) && (recLen >0)
                && (pageNo.pid == curPage.pid))
        {

            offset = getSlotOffset (slotNo);
            Quadruple quadruple = new Quadruple(data, recLen);
            return quadruple;
        }

        else {
            throw new InvalidSlotNumberException (null, "HEAPFILE: INVALID_SLOTNO");
        }

    }

    protected void compact_slot_dir()
            throws IOException
    {
        int  current_scan_posn = 0;   // current scan position
        int  first_free_slot   = -1;   // An invalid position.
        boolean move = false;          // Move a record? -- initially false
        short length;
        short offset;

        slotCnt = Convert.getShortValue (SLOT_CNT, data);
        freeSpace = Convert.getShortValue (FREE_SPACE, data);

        while (current_scan_posn < slotCnt)
        {
            length = getSlotLength (current_scan_posn);

            if ((length == EMPTY_SLOT) && (move == false))
            {
                move = true;
                first_free_slot = current_scan_posn;
            }
            else if ((length != EMPTY_SLOT) && (move == true))
            {
                offset = getSlotOffset (current_scan_posn);

                // slot[first_free_slot].length = slot[current_scan_posn].length;
                // slot[first_free_slot].offset = slot[current_scan_posn].offset;
                setSlot ( first_free_slot, length, offset);

                // Mark the current_scan_posn as empty
                //  slot[current_scan_posn].length = EMPTY_SLOT;
                setSlot (current_scan_posn, EMPTY_SLOT, 0);

                // Now make the first_free_slot point to the next free slot.
                first_free_slot++;

                // slot[current_scan_posn].length == EMPTY_SLOT !!
                while (getSlotLength (first_free_slot) != EMPTY_SLOT)
                {
                    first_free_slot++;
                }
            }

            current_scan_posn++;
        }

        if (move == true)
        {
            // Adjust amount of free space on page and slotCnt
            freeSpace += SIZE_OF_SLOT * (slotCnt - first_free_slot);
            slotCnt = (short) first_free_slot;
            Convert.setShortValue (freeSpace, FREE_SPACE, data);
            Convert.setShortValue (slotCnt, SLOT_CNT, data);
        }
    }

}
