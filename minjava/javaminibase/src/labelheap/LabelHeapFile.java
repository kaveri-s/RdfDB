package labelheap;

import java.io.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import quadrupleheap.*;

interface  Filetype {
    int TEMP = 0;
    int ORDINARY = 1;

} // end of Filetype
public class LabelHeapFile implements Filetype,  GlobalConst {

    PageId      _firstDirPageId;   // page number of header page
    int         _ftype;
    private     boolean     _file_deleted;
    private     String 	 _fileName;
    private static int tempfilecount = 0;

    public LabelHeapFile (String name) throws HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException {
        _file_deleted = true;
        _fileName = null;

        if(name == null)
        {
            // If the name is NULL, allocate a temporary name
            // and no logging is required.
            _fileName = "tempLHeapFile";
            String useId = new String("user.name");
            String userAccName;
            userAccName = System.getProperty(useId);
            _fileName = _fileName + userAccName;

            String filenum = Integer.toString(tempfilecount);
            _fileName = _fileName + filenum;
            _ftype = TEMP;
            tempfilecount ++;

        }
        else
        {
            _fileName = name;
            _ftype = ORDINARY;
        }
        
        // try to open the file
        Page apage = new Page();
        _firstDirPageId = null;
        if (_ftype == ORDINARY)
            _firstDirPageId = get_file_entry(_fileName);

        if(_firstDirPageId==null)
        {
            // file doesn't exist. First create it.
            _firstDirPageId = newPage(apage, 1);
            // check error
            if(_firstDirPageId == null)
                throw new HFException(null, "can't new page");

            add_file_entry(_fileName, _firstDirPageId);
            // check error(new exception: Could not add file entry

            LHFPage firstDirPage = new LHFPage();
            firstDirPage.init(_firstDirPageId, apage);
            PageId pageId = new PageId(INVALID_PAGE);

            firstDirPage.setNextPage(pageId);
            firstDirPage.setPrevPage(pageId);
            unpinPage(_firstDirPageId, true /*dirty*/ );


        }
        _file_deleted = false;
    }

    public void deleteFile() throws InvalidSlotNumberException,
            FileAlreadyDeletedException,
            InvalidTupleSizeException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException{
        if(_file_deleted )
            throw new FileAlreadyDeletedException(null, "file already deleted");
        // Mark the deleted flag (even if it doesn't get all the way done).
        _file_deleted = true;

        // Deallocate all data pages
        PageId currentDirPageId = new PageId();
        currentDirPageId.pid = _firstDirPageId.pid;
        PageId nextDirPageId = new PageId();
        nextDirPageId.pid = 0;
        Page pageinbuffer = new Page();
        THFPage currentDirPage =  new THFPage();
        Quadruple aquad;

        pinPage(currentDirPageId, currentDirPage, false);

        LID lid = new LID();
        while(currentDirPageId.pid != INVALID_PAGE)
        {
            for(lid = currentDirPage.firstRecord();
                lid != null;
                lid = currentDirPage.nextRecord(lid))
            {
                alabel = currentDirPage.getRecord(lid);
                DataPageInfo dpinfo = new DataPageInfo( alabel); //Change class
                freePage(dpinfo.pageId);
            }
            // ASSERTIONS:
            // - we have freePage()'d all data pages referenced by
            // the current directory page.
            nextDirPageId = currentDirPage.getNextPage();
            freePage(currentDirPageId);
            currentDirPageId.pid = nextDirPageId.pid;

            if (nextDirPageId.pid != INVALID_PAGE)
            {
                pinPage(currentDirPageId, currentDirPage, false);
                //currentDirPage.openHFpage(pageinbuffer);
            }
        }
        delete_file_entry( _fileName );
    }

    public boolean deleteLabel(LID lid) throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFException,
            HFBufMgrException,
            HFDiskMgrException,
            Exception{
        boolean status;
        LHFPage currentDirPage = new LHFPage();
        PageId currentDirPageId = new PageId();
        LHFPage currentDataPage = new LHFPage();
        PageId currentDataPageId = new PageId();
        LID currentDataPageLid = new LID();

        status = _findDataPage(lid,
                currentDirPageId, currentDirPage,
                currentDataPageId, currentDataPage,
                currentDataPageLid);
        if(status != true) return status;	// record not found

        // ASSERTIONS:
        // - currentDirPage, currentDirPageId valid and pinned
        // - currentDataPage, currentDataPageid valid and pinned

        // get datapageinfo from the current directory page:
        Label alabel;
        alabel = currentDirPage.returnRecord(currentDataPageLid);
        quadrupleheap.DataPageInfo pdpinfo = new quadrupleheap.DataPageInfo(alabel);

        // delete the record on the datapage
        currentDataPage.deleteRecord(alabel);

        pdpinfo.recct--;
        pdpinfo.flushToTuple();	//Write to the buffer pool
        if (pdpinfo.recct >= 1) {
            // more records remain on datapage so it still hangs around.
            // we just need to modify its directory entry
            pdpinfo.availspace = currentDataPage.available_space();
            pdpinfo.flushToTuple();
            unpinPage(currentDataPageId, true /* = DIRTY*/);
            unpinPage(currentDirPageId, true /* = DIRTY */);
        }
        else {
            // the record is already deleted:
            // we're removing the last record on datapage so free datapage
            // also, free the directory page if
            //   a) it's not the first directory page, and
            //   b) we've removed the last DataPageInfo record on it.

            // delete empty datapage
            unpinPage(currentDataPageId, false /*undirty*/);
            freePage(currentDataPageId);

            // delete corresponding DataPageInfo-entry on the directory page:
            // currentDataPageRid points to datapage (from for loop above)
            currentDirPage.deleteRecord(currentDataPageQid);

            // now check whether the directory page is empty:
            currentDataPageQid = currentDirPage.firstRecord();

            // st == OK: we still found a datapageinfo record on this directory page
            PageId pageId;
            pageId = currentDirPage.getPrevPage();
            if((currentDataPageQid == null)&&(pageId.pid != INVALID_PAGE))
            {
                // the directory-page is not the first directory page and it is empty:
                // delete it

                // point previous page around deleted page:

                THFPage prevDirPage = new THFPage();
                pinPage(pageId, prevDirPage, false);
                pageId = currentDirPage.getNextPage();
                prevDirPage.setNextPage(pageId);
                pageId = currentDirPage.getPrevPage();
                unpinPage(pageId, true /* = DIRTY */);

                // set prevPage-pointer of next Page
                pageId = currentDirPage.getNextPage();
                if(pageId.pid != INVALID_PAGE)
                {
                    THFPage nextDirPage = new THFPage();
                    pageId = currentDirPage.getNextPage();
                    pinPage(pageId, nextDirPage, false);
                    //nextDirPage.openHFpage(apage);
                    pageId = currentDirPage.getPrevPage();
                    nextDirPage.setPrevPage(pageId);
                    pageId = currentDirPage.getNextPage();
                    unpinPage(pageId, true /* = DIRTY */);
                }
                // delete empty directory page: (automatically unpinned?)
                unpinPage(currentDirPageId, false/*undirty*/);
                freePage(currentDirPageId);
            }
            else
            {
                // either (the directory page has at least one more datapagerecord
                // entry) or (it is the first directory page):
                // in both cases we do not delete it, but we have to unpin it:
                unpinPage(currentDirPageId, true /* == DIRTY */);
            }
        }
        return true;
    }

    public int getLabelCnt() throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFDiskMgrException,
            HFBufMgrException,
            IOException {
        int answer = 0;
        PageId currentDirPageId = new PageId(_firstDirPageId.pid);
        PageId nextDirPageId = new PageId(0);
        LHFPage currentDirPage = new LHFPage();

        while(currentDirPageId.pid != INVALID_PAGE)
        {
            pinPage(currentDirPageId, currentDirPage, false);

            LID lid = new LID();
            Label alabel;

            for (lid = currentDirPage.firstRecord();
                 lid != null;	// lid==NULL means no more record
                 lid = currentDirPage.nextRecord(lid))
            {
                alabel = currentDirPage.getRecord(lid);
                quadrupleheap.DataPageInfo dpinfo = new quadrupleheap.DataPageInfo(alabel);
                answer += dpinfo.recct;
            }
            // ASSERTIONS: no more record
            // - we have read all datapage records on
            //   the current directory page.
            nextDirPageId = currentDirPage.getNextPage();
            unpinPage(currentDirPageId, false /*undirty*/);
            currentDirPageId.pid = nextDirPageId.pid;
        }
        // ASSERTIONS:
        // - if error, exceptions
        // - if end of heapfile reached: currentDirPageId == INVALID_PAGE
        // - if not yet end of heapfile: currentDirPageId valid
        return answer;
    }

    public String getLabel(LID lid)
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFException,
            HFDiskMgrException,
            HFBufMgrException,
            Exception{
        boolean status;
        LHFPage dirPage = new LHFPage();
        PageId currentDirPageId = new PageId();
        LHFPage dataPage = new LHFPage();
        PageId currentDataPageId = new PageId();
        LID currentDataPageLid = new LID();

        status = _findDataPage(lid,
                currentDirPageId, dirPage,
                currentDataPageId, dataPage,
                currentDataPageLid);

        if(status != true) return null; // record not found

        Label alabel = new Label();
        alabel = dataPage.getRecord(lid);

        /*
         * getRecord has copied the contents of lid into recPtr and fixed up
         * recLen also.  We simply have to unpin dirpage and datapage which
         * were originally pinned by _findDataPage.
         */
        unpinPage(currentDataPageId,false /*undirty*/);
        unpinPage(currentDirPageId,false /*undirty*/);

        return  alabel.name;  //(true?)OK, but the caller need check if atuple==NULL
    }

    public LID insertLabel(String Label)throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            SpaceNotAvailableException,
            HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException {
        int dpinfoLen = 0;
        int labLen = Label.length;
        boolean found;
        LID currentDataPageLid = new LID();
        Page pageinbuffer = new Page();
        LHFPage currentDirPage = new LHFPage();
        LHFPage currentDataPage = new LHFPage();

        LHFPage nextDirPage = new LHFPage();
        PageId currentDirPageId = new PageId(_firstDirPageId.pid);
        PageId nextDirPageId = new PageId();  // OK

        pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);

        found = false;
        Label alabel;
        DataPageInfo dpinfo = new DataPageInfo();
        while (found == false)
        { //Start While01
            // look for suitable dpinfo-struct
            for (currentDataPageLid = currentDi
                 rPage.firstRecord();
                 currentDataPageLid != null;
                 currentDataPageLid =
                         currentDirPage.nextRecord(currentDataPageLid))
            {
                alabel = currentDirPage.getRecord(currentDataPageLid);
                dpinfo = new quadrupleheap.DataPageInfo(alabel);

                // need check the record length == DataPageInfo'slength

                if(labLen <= dpinfo.availspace)
                {
                    found = true;
                    break;
                }
            }

            // two cases:
            // (1) found == true:
            //     currentDirPage has a datapagerecord which can accomodate
            //     the record which we have to insert
            // (2) found == false:
            //     there is no datapagerecord on the current directory page
            //     whose corresponding datapage has enough space free
            //     several subcases: see below
            if(found == false)
            { //Start IF01
                // case (2)

                //System.out.println("no datapagerecord on the current directory is OK");
                //System.out.println("dirpage availspace "+currentDirPage.available_space());

                // on the current directory page is no datapagerecord which has
                // enough free space
                //
                // two cases:
                //
                // - (2.1) (currentDirPage->available_space() >= sizeof(DataPageInfo):
                //         if there is enough space on the current directory page
                //         to accomodate a new datapagerecord (type DataPageInfo),
                //         then insert a new DataPageInfo on the current directory
                //         page
                // - (2.2) (currentDirPage->available_space() <= sizeof(DataPageInfo):
                //         look at the next directory page, if necessary, create it.

                if(currentDirPage.available_space() >= dpinfo.size)
                {
                    //Start IF02
                    // case (2.1) : add a new data page record into the
                    //              current directory page
                    currentDataPage = _newDatapage(dpinfo);
                    // currentDataPage is pinned! and dpinfo->pageId is also locked
                    // in the exclusive mode

                    // didn't check if currentDataPage==NULL, auto exception


                    // currentDataPage is pinned: insert its record
                    // calling a THFPage function



                    alabel = dpinfo.convertToLabel();

                    String tmpData = alabel.getLabel();
                    currentDataPageLid = currentDirPage.insertRecord(tmpData);

                    LID tmplid = currentDirPage.firstRecord();


                    // need catch error here!
                    if(currentDataPageLid == null)
                        throw new HFException(null, "no space to insert rec.");

                    // end the loop, because a new datapage with its record
                    // in the current directorypage was created and inserted into
                    // the heapfile; the new datapage has enough space for the
                    // record which the user wants to insert

                    found = true;

                } //end of IF02
                else
                {  //Start else 02
                    // case (2.2)
                    nextDirPageId = currentDirPage.getNextPage();
                    // two sub-cases:
                    //
                    // (2.2.1) nextDirPageId != INVALID_PAGE:
                    //         get the next directory page from the buffer manager
                    //         and do another look
                    // (2.2.2) nextDirPageId == INVALID_PAGE:
                    //         append a new directory page at the end of the current
                    //         page and then do another loop

                    if (nextDirPageId.pid != INVALID_PAGE)
                    { //Start IF03
                        // case (2.2.1): there is another directory page:
                        unpinPage(currentDirPageId, false);

                        currentDirPageId.pid = nextDirPageId.pid;

                        pinPage(currentDirPageId,
                                currentDirPage, false);



                        // now go back to the beginning of the outer while-loop and
                        // search on the current directory page for a suitable datapage
                    } //End of IF03
                    else
                    {  //Start Else03
                        // case (2.2): append a new directory page after currentDirPage
                        //             since it is the last directory page
                        nextDirPageId = newPage(pageinbuffer, 1);
                        // need check error!
                        if(nextDirPageId == null)
                            throw new HFException(null, "can't new page");

                        // initialize new directory page
                        nextDirPage.init(nextDirPageId, pageinbuffer);
                        PageId temppid = new PageId(INVALID_PAGE);
                        nextDirPage.setNextPage(temppid);
                        nextDirPage.setPrevPage(currentDirPageId);

                        // update current directory page and unpin it
                        // currentDirPage is already locked in the Exclusive mode
                        currentDirPage.setNextPage(nextDirPageId);
                        unpinPage(currentDirPageId, true/*dirty*/);

                        currentDirPageId.pid = nextDirPageId.pid;
                        currentDirPage = new THFPage(nextDirPage);

                        // remark that MINIBASE_BM->newPage already
                        // pinned the new directory page!
                        // Now back to the beginning of the while-loop, using the
                        // newly created directory page.

                    } //End of else03
                } // End of else02
                // ASSERTIONS:
                // - if found == true: search will end and see assertions below
                // - if found == false: currentDirPage, currentDirPageId
                //   valid and pinned

            }//end IF01
            else
            { //Start else01
                // found == true:
                // we have found a datapage with enough space,
                // but we have not yet pinned the datapage:

                // ASSERTIONS:
                // - dpinfo valid

                // System.out.println("find the dirpagerecord on current page");

                pinPage(dpinfo.pageId, currentDataPage, false);
                //currentDataPage.openHFpage(pageinbuffer);


            }//End else01
        } //end of While01

        // ASSERTIONS:
        // - currentDirPageId, currentDirPage valid and pinned
        // - dpinfo.pageId, currentDataPageRid valid
        // - currentDataPage is pinned!

        if ((dpinfo.pageId).pid == INVALID_PAGE) // check error!
            throw new HFException(null, "invalid PageId");

        if (!(currentDataPage.available_space() >= labLen))
            throw new SpaceNotAvailableException(null, "no available space");

        if (currentDataPage == null)
            throw new HFException(null, "can't find Data page");


        LID lid;
        lid = currentDataPage.insertRecord(Label);

        dpinfo.recct++;
        dpinfo.availspace = currentDataPage.available_space();

        unpinPage(dpinfo.pageId, true /* = DIRTY */);

        // DataPage is now released
        alabel = currentDirPage.returnRecord(currentDataPageLid);
        quadrupleheap.DataPageInfo dpinfo_ondirpage = new quadrupleheap.DataPageInfo(alabel);

        dpinfo_ondirpage.availspace = dpinfo.availspace;
        dpinfo_ondirpage.recct = dpinfo.recct;
        dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;
        dpinfo_ondirpage.flushToTuple();

        unpinPage(currentDirPageId, true /* = DIRTY */);

        return lid;
    }

    public LScan openScan()
            throws InvalidTupleSizeException,
            IOException
    {
        LScan newscan = new LScan(this);
        return newscan;
    }

    boolean updateLabel(LID lid, String newLabel)
            throws InvalidSlotNumberException,
            InvalidUpdateException,
            InvalidTupleSizeException,
            HFException,
            HFDiskMgrException,
            HFBufMgrException,
            Exception{
        boolean status;
        LHFPage dirPage = new LHFPage();
        PageId currentDirPageId = new PageId();
        LHFPage dataPage = new LHFPage();
        PageId currentDataPageId = new PageId();
        LID currentDataPageQid = new LID();

        status = _findDataPage(lid,
                currentDirPageId, dirPage,
                currentDataPageId, dataPage,
                currentDataPageQid);

        if(status != true) return status;	// record not found
        Label alabel = new Label();
        alabel = dataPage.returnRecord(lid);

        // new copy of this record fits in old space;
        alabel.setLabel(newLabel);
        unpinPage(currentDataPageId, true /* = DIRTY */);
        unpinPage(currentDirPageId, false /*undirty*/);

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
            throw new HFBufMgrException(e,"QuadrupleHeapFile.java: pinPage() failed");
        }

    }

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
            throw new HFBufMgrException(e,"QuadrupleHeapFile.java: unpinPage() failed");
        }

    } // end of unpinPage

    private void freePage(PageId pageno)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.freePage(pageno);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"QuadrupleHeapFile.java: freePage() failed");
        }

    } // end of freePage

    private PageId newPage(Page page, int num)
            throws HFBufMgrException {

        PageId tmpId = new PageId();

        try {
            tmpId = SystemDefs.JavabaseBM.newPage(page,num);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"QuadrupleHeapFile.java: newPage() failed");
        }

        return tmpId;

    } // end of newPage
    
    private PageId get_file_entry(String filename)
            throws HFDiskMgrException {

        PageId tmpId = new PageId();

        try {
            tmpId = SystemDefs.JavabaseDB.get_file_entry(filename);
        }
        catch (Exception e) {
            throw new HFDiskMgrException(e,"QuadrupleHeapFile.java: get_file_entry() failed");
        }

        return tmpId;

    } // end of get_file_entry
    
    private void add_file_entry(String filename, PageId pageno)
            throws HFDiskMgrException {

        try {
            SystemDefs.JavabaseDB.add_file_entry(filename,pageno);
        }
        catch (Exception e) {
            throw new HFDiskMgrException(e,"QuadrupleHeapFile.java: add_file_entry() failed");
        }

    } // end of add_file_entry

    private void delete_file_entry(String filename)
            throws HFDiskMgrException {

        try {
            SystemDefs.JavabaseDB.delete_file_entry(filename);
        }
        catch (Exception e) {
            throw new HFDiskMgrException(e,"QuadrupleHeapFile.java: delete_file_entry() failed");
        }

    } // end of delete_file_entry
}
