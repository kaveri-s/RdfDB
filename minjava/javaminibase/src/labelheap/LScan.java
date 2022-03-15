package labelheap;

import diskmgr.*;
import global.*;
import heap.*;

import java.io.IOException;

public class LScan implements GlobalConst {

    /**
     * Note that one record in our way-cool HeapFile implementation is
     * specified by six (6) parameters, some of which can be determined
     * from others:
     */

    /** The heapfile we are using. */
    private LabelHeapFile _lhf;

    /** PageId of current directory page (which is itself an HFPage) */
    private PageId dirpageId = new PageId();

    /** pointer to in-core data of dirpageId (page is pinned) */
    private HFPage dirpage = new HFPage();

    /** record ID of the DataPageInfo struct (in the directory page) which
     * describes the data page where our current record lives.
     */
    private RID datapageRid = new RID();

    /** the actual PageId of the data page with the current record */
    private PageId datapageId = new PageId();

    /** in-core copy (pinned) of the same */
    private LHFPage datapage = new LHFPage();

    /** record ID of the current record (from the current data page) */
    private LID userlid = new LID();

    /** Status of next user status */
    private boolean nextUserStatus;


    /** The constructor pins the first directory page in the file
     * and initializes its private data members from the private
     * data member from hf
     *
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     *
     * @param lhf A HeapFile object
     */
    public LScan(LabelHeapFile lhf)
            throws InvalidTupleSizeException,
            IOException
    {
        _lhf = lhf;
        firstDataPage();
    }

    /** Retrieve the next record in a sequential scan
     *
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     *
     * @param lid Record ID of the record
     * @return the Label of the retrieved record.
     */
    public Label getNext(LID lid)
            throws InvalidTupleSizeException,
            IOException
    {
        Label recquadruple = null;

        if (nextUserStatus != true) {
            nextDataPage();
        }

        if (datapage == null)
            return null;

        lid.pageNo.pid = userlid.pageNo.pid;
        lid.slotNo = userlid.slotNo;

        try {
            recquadruple = datapage.getRecord(lid);
        }

        catch (Exception e) {
            e.printStackTrace();
        }

        userlid = datapage.nextRecord(lid);
        if(userlid == null) nextUserStatus = false;
        else nextUserStatus = true;

        return recquadruple;
    }


    /** Position the scan cursor to the record with the given lid.
     *
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     * @param lid Record ID of the given record
     * @return 	true if successful,
     *			false otherwise.
     */
    public boolean position(LID lid)
            throws InvalidTupleSizeException,
            IOException
    {
        LID     nxtlid = new LID();
        boolean bst;

        if (nxtlid.equals(lid)==true)
            return true;

        // This is kind lame, but otherwise it will take all day.
        PageId pgid = new PageId();
        pgid.pid = lid.pageNo.pid;

        if (!datapageId.equals(pgid)) {

            // reset everything and start over from the beginning
            reset();

            bst =  firstDataPage();
            if (bst != true)
                return bst;

            while (!datapageId.equals(pgid)) {
                bst = nextDataPage();
                if (bst != true)
                    return bst;
            }
        }

        // Now we are on the correct page.

        try{
            userlid = datapage.firstRecord();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        if (userlid == null)
        {
            bst = false;
            return bst;
        }

        bst = peekNext(nxtlid);

        while ((bst == true) && (nxtlid != lid))
            bst = mvNext(nxtlid);

        return bst;
    }

    /** Closes the TScan object */
    public void closescan()
    {
        reset();
    }

    /** Reset everything and unpin all pages. */
    private void reset()
    {

        if (datapage != null) {
            try{
                unpinPage(datapageId, false);
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
        datapageId.pid = 0;
        datapage = null;

        if (dirpage != null) {
            try{
                unpinPage(dirpageId, false);
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
        dirpage = null;
        nextUserStatus = true;
    }


    /** Move to the first data page in the file.
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     * @return true if successful
     *         false otherwise
     */
    private boolean firstDataPage()
            throws InvalidTupleSizeException,
            IOException
    {
        DataPageInfo dpinfo;
        Tuple rectuple = null;

        /** copy data about first directory page */
        dirpageId.pid = _lhf._firstDirPageId.pid;
        nextUserStatus = true;

        /** get first directory page and pin it */
        try {
            dirpage  = new HFPage();
            pinPage(dirpageId, dirpage, false);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        /** now try to get a pointer to the first datapage */
        datapageRid = dirpage.firstRecord();

        if (datapageRid != null) {
            /** there is a datapage record on the first directory page: */

            try {
                rectuple = dirpage.getRecord(datapageRid);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            dpinfo = new DataPageInfo(rectuple);
            datapageId.pid = dpinfo.pageId.pid;

        } else {

            /** the first directory page is the only one which can possibly remain
             * empty: therefore try to get the next directory page and
             * check it. The next one has to contain a datapage record, unless
             * the heapfile is empty:
             */
            PageId nextDirPageId = dirpage.getNextPage();

            if (nextDirPageId.pid != INVALID_PAGE) {

                try {
                    unpinPage(dirpageId, false);
                    dirpage = null;
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                try {

                    dirpage = new HFPage();
                    pinPage(nextDirPageId, dirpage, false);

                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                /** now try again to read a data record: */

                try {
                    datapageRid = dirpage.firstRecord();
                }

                catch (Exception e) {
                    e.printStackTrace();
                    datapageId.pid = INVALID_PAGE;
                }

                if(datapageRid != null) {

                    try {

                        rectuple = dirpage.getRecord(datapageRid);
                    }

                    catch (Exception e) {
                        //    System.err.println("SCAN: Error getRecord 4: " + e);
                        e.printStackTrace();
                    }

                    if (rectuple.getLength() != DataPageInfo.size)
                        return false;

                    dpinfo = new DataPageInfo(rectuple);
                    datapageId.pid = dpinfo.pageId.pid;

                } else {
                    // heapfile empty
                    datapageId.pid = INVALID_PAGE;
                }
            }//end if01
            else {// heapfile empty
                datapageId.pid = INVALID_PAGE;
            }
        }

        datapage = null;

        try{
            nextDataPage();
        }

        catch (Exception e) {
            //  System.err.println("SCAN Error: 1st_next 0: " + e);
            e.printStackTrace();
        }

        return true;

    }


    /** Move to the next data page in the file and
     * retrieve the next data page.
     *
     * @return 		true if successful
     *			false if unsuccessful
     */
    private boolean nextDataPage()
            throws InvalidTupleSizeException,
            IOException
    {
        boolean nextDataPageStatus;
        DataPageInfo dpinfo;
        PageId nextDirPageId;
        Tuple rectuple = null;

        if ((dirpage == null) && (datapageId.pid == INVALID_PAGE))
            return false;

        if (datapage == null) {
            if (datapageId.pid == INVALID_PAGE) {
                // heapfile is empty to begin with

                try{
                    unpinPage(dirpageId, false);
                    dirpage = null;
                }
                catch (Exception e){
                    e.printStackTrace();
                }

            } else {

                // pin first data page
                try {
                    datapage  = new LHFPage();
                    pinPage(datapageId, datapage, false);
                }
                catch (Exception e){
                    e.printStackTrace();
                }

                try {
                    userlid = datapage.firstRecord();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                return true;
            }
        }

        try{
            unpinPage(datapageId, false /* no dirty */);
            datapage = null;
        }
        catch (Exception e){

        }

        if (dirpage == null) {
            return false;
        }

        datapageRid = dirpage.nextRecord(datapageRid);

        if (datapageRid == null) {
            nextDataPageStatus = false;

            nextDirPageId = dirpage.getNextPage();

            // unpin the current directory page
            try {
                unpinPage(dirpageId, false /* not dirty */);
                dirpage = null;

                datapageId.pid = INVALID_PAGE;
            }

            catch (Exception e) {

            }

            if (nextDirPageId.pid == INVALID_PAGE)
                return false;
            else {

                dirpageId = nextDirPageId;

                try {
                    dirpage  = new HFPage();
                    pinPage(dirpageId, (Page)dirpage, false);
                }

                catch (Exception e){

                }

                if (dirpage == null)
                    return false;

                try {
                    datapageRid = dirpage.firstRecord();
                    nextDataPageStatus = true;
                }
                catch (Exception e){
                    nextDataPageStatus = false;
                    return false;
                }
            }
        }

        try {
            rectuple = dirpage.getRecord(datapageRid);
        }

        catch (Exception e) {
            System.err.println("HeapFile: Error in Scan" + e);
        }

        if (rectuple.getLength() != DataPageInfo.size)
            return false;

        dpinfo = new DataPageInfo(rectuple);
        datapageId.pid = dpinfo.pageId.pid;

        try {
            datapage = new LHFPage();
            pinPage(dpinfo.pageId, datapage, false);
        }

        catch (Exception e) {
            System.err.println("HeapFile: Error in Scan" + e);
        }

        userlid = datapage.firstRecord();

        if(userlid == null)
        {
            nextUserStatus = false;
            return false;
        }

        return true;
    }

    private boolean peekNext(LID lid) {

        lid.pageNo.pid = userlid.pageNo.pid;
        lid.slotNo = userlid.slotNo;
        return true;

    }


    /** Move to the next record in a sequential scan.
     * Also returns the RID of the (new) current record.
     */
    private boolean mvNext(LID lid)
            throws InvalidTupleSizeException,
            IOException
    {
        LID nextlid;
        boolean status;

        if (datapage == null)
            return false;

        nextlid = datapage.nextRecord(lid);

        if( nextlid != null ){
            userlid.pageNo.pid = nextlid.pageNo.pid;
            userlid.slotNo = nextlid.slotNo;
            return true;
        } else {

            status = nextDataPage();

            if (status==true){
                lid.pageNo.pid = userlid.pageNo.pid;
                lid.slotNo = userlid.slotNo;
            }

        }
        return true;
    }

    /**
     * short cut to access the pinPage function in bufmgr package.
     * @see bufmgr.pinPage
     */
    private void pinPage(PageId pageno, Page page, boolean emptyPage)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"TScan.java: pinPage() failed");
        }

    } // end of pinPage

    /**
     * short cut to access the unpinPage function in bufmgr package.
     * @see bufmgr.unpinPage
     */
    private void unpinPage(PageId pageno, boolean dirty)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"TScan.java: unpinPage() failed");
        }

    } // end of unpinPage



}
