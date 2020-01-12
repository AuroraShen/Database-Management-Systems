/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_exists_exception.h"

//#define DEBUG

namespace badgerdb{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------
/**
 * BTreeIndex Constructor.
 * Check to see if the corresponding index file exists. If so, open the file.
 * If not, create it and insert entries for every tuple in the base relation using FileScan class.
 *
 * @param relationName        Name of file.
 * @param outIndexName        Return the name of index file.
 * @param bufMgrIn            Buffer Manager Instance
 * @param attrByteOffset      Offset of attribute, over which index is to be built, in the record
 * @param attrType            Datatype of attribute over which index is built
 */
BTreeIndex::BTreeIndex(const std::string &relationName,
        std::string &outIndexName,
        BufMgr *bufMgrIn,
        const int attrByteOffset,
        const Datatype attrType){

    // Construct from the gobal
    this->bufMgr = bufMgrIn;
    this->attrByteOffset = attrByteOffset;
    this->attributeType = attrType;

    leafOccupancy = INTARRAYLEAFSIZE;
    nodeOccupancy = INTARRAYNONLEAFSIZE;
    scanExecuting = false;

    std::ostringstream idxString;
    idxString << relationName << '.' << attrByteOffset;
    std::string indexName = idxString.str(); // indexName is the name of the index file
    outIndexName = indexName;

    IndexMetaInfo* metadata;
    Page *headerPage;
    Page *rootPage;

    // Check if the specified index file opened and its existence
    try {
        file = new BlobFile(outIndexName, false);
        headerPageNum = file->getFirstPageNo();
        // Read file by calling the read page in the buffer manager
        bufMgr->readPage(file, headerPageNum, headerPage);
        // Metdata of the header page
        metadata = (IndexMetaInfo *) headerPage;
        rootPageNum = metadata->rootPageNo;

        // Check index information
        if (strcmp(metadata->relationName, relationName.c_str()) != 0 ||
            metadata->attrType != attrType || metadata->attrByteOffset != attrByteOffset){
            // UnPin by calling unPinPage in the buffer manager
            // Unpin by turning  dirty off
            bufMgr->unPinPage(file, headerPageNum, false);
            throw BadIndexInfoException(outIndexName);
        }

        // UnPin as soon as you can
        bufMgr->unPinPage(file, headerPageNum, false);
    }
    catch (FileNotFoundException e) {
        // Creat new file if File not found
        file = new BlobFile(outIndexName, true);
        bufMgr->allocPage(file, headerPageNum, headerPage);
        bufMgr->allocPage(file, rootPageNum, rootPage);

        metadata = (IndexMetaInfo *) headerPage;
        strncpy((char *) (&(metadata->relationName)), relationName.c_str(), 20);
        metadata->relationName[19] = 0;

        metadata->attrByteOffset = attrByteOffset;
        metadata->attrType = attrType;
        metadata->rootPageNo = rootPageNum;

        initialRootPageNum = rootPageNum;
        LeafNodeInt *root = (LeafNodeInt *) rootPage;
        root->rightSibPageNo = 0;

        try {
            // Scan the new file
            FileScan fileScan(relationName, bufMgr);
            RecordId scanRid = {};
            while (1) {
                // By using scanNext
                fileScan.scanNext(scanRid);
                std::string recordString = fileScan.getRecord();
                insertEntry((int*)recordString.c_str() + attrByteOffset, scanRid);
            }
        }
        catch (EndOfFileException e) {
            bufMgr->flushFile(file);
        }
        // UnPin as soon as you can
        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, rootPageNum, true);
    }

}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------
/**
 * BTreeIndex Destructor, it performs any cleanup that may be necessary
 * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
 * and delete file instance thereby closing the index file.
 * Destructor not throw any exceptions. All exceptions should be caught in here itself.
 * */
BTreeIndex::~BTreeIndex()
{
    if (scanExecuting)
        endScan();
    // Flush index file by calling flushFile in buffer
    bufMgr->flushFile(file);
    delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
/**
 * Insert a new entry using the pair <value,rid>.
 * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
 * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
 * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
 * Make sure to unpin pages as soon as you can.
 *
 * @param key     Key to insert, pointer to integer/double/char string
 * @param rid     Record ID of a record whose entry is getting inserted into the index.
**/
const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
    // Record Id entry setup
    RIDKeyPair<int> entry;
    entry.set(rid, *((int *) key));

    // read current page
    Page *current;
    bufMgr->readPage(file, rootPageNum, current);

    // New Child entry setup
    PageKeyPair<int> *newEntry = nullptr;
    insertion(current, rootPageNum, entry, newEntry, initialRootPageNum == rootPageNum ? true : false);
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertion
// -----------------------------------------------------------------------------
/**
 * Recursively perform insertion with different cases, the helper method perform the most important
 * insertion of the b+ tree
 *
 * @param current       the current Page given
 * @param currPageNum   the current Page Id given
 * @param entry     the index entry given to be inserted
 * @param newEntry      the new entry which is a page key pair pushed up to after splitting
 * @param isLeaf        whether the current page is a leaf node
 */
const void BTreeIndex::insertion(Page *current, PageId currPageNum, const RIDKeyPair<int> entry,
        PageKeyPair<int> *&newEntry,
        bool isLeaf)
    {
    // Insertion case for non leaf node
    if (!isLeaf) {
        // Casting the current non leaf node
        NonLeafNodeInt *node = (NonLeafNodeInt *) current;
        // Turn to next page
        Page *nextPage;
        PageId nextNode;
        findNext(node, nextNode, entry.key);
        bufMgr->readPage(file, nextNode, nextPage);
        // Set next insertion isLeaf to true for leaf case
        isLeaf = node->level == 1;
        insertion(nextPage, nextNode, entry, newEntry,isLeaf);

        // Other cases
        if (newEntry == nullptr) {
            // There is no split and no new entry, Unpin as soon as you can
            bufMgr->unPinPage(file, currPageNum, false);
        }
        // Current node not full, calls nonLeafInsertion
        else if(node->pageNoArray[nodeOccupancy] == 0) {
            nonLeafInsertion(node, newEntry);
            newEntry = nullptr;
            // UnPin as soon as you can
            bufMgr->unPinPage(file, currPageNum, true);
        }
        // Current node is full, split needed
        else if(node->pageNoArray[nodeOccupancy] != 0){
            splitNonLeaf(node, currPageNum, newEntry);
        }
    }

    // Insertion case for leaf node
    else {
        LeafNodeInt *node = (LeafNodeInt *) current;
        // Perform leaf insertion
        if (node->ridArray[leafOccupancy - 1].page_number == 0) {
            leafInsertion(node, entry);
            newEntry = nullptr;
            // Unpin as soon as you can
            bufMgr->unPinPage(file, currPageNum, true);
        }
        // Split needed
        else {
            splitLeaf(node, currPageNum, newEntry, entry);
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitNonLeaf
// -----------------------------------------------------------------------------
/**
 * Split the given non leaf node. It moves the values stored in the
 * given node after the split index into a new non leaf node.
 *
 * @param node       the node given we will split from
 * @param pageId     the page ID of the node given
 * @param newEntry   the new entry which is a page key pair pushed up to after splitting
*/
const void BTreeIndex::splitNonLeaf(NonLeafNodeInt *node, PageId pageId, PageKeyPair<int> *&newEntry) {
    // Allocate a new page
    Page *newPage;
    PageId newPageId;
    bufMgr->allocPage(file, newPageId, newPage);
    NonLeafNodeInt *newNode = (NonLeafNodeInt *) newPage;

    // Set up the mid point and push entry
    int midPt = nodeOccupancy / 2;
    int pushIndex = midPt;
    PageKeyPair<int> pushEntry;

    if (nodeOccupancy % 2 == 0)
        pushIndex = newEntry->key < node->keyArray[midPt] ? midPt - 1 : midPt;
    pushEntry.set(newPageId, node->keyArray[pushIndex]);

    // New mid point fro recursion
    midPt = pushIndex + 1;
    for (int i = midPt; i < nodeOccupancy; i++) {
        newNode->pageNoArray[i - midPt] = node->pageNoArray[i + 1];
        newNode->keyArray[i - midPt] = node->keyArray[i];

        node->pageNoArray[i + 1] = (PageId) 0;
        node->keyArray[i + 1] = 0;
    }

    // Back to the original node for performing insertion
    newNode->level = node->level;
    node->keyArray[pushIndex] = 0;
    node->pageNoArray[pushIndex] = (PageId) 0;
    if(newEntry->key < newNode->keyArray[0])
        nonLeafInsertion( node, newEntry);
    else
        nonLeafInsertion( newNode, newEntry);

    // Updating root after insertion
    newEntry = &pushEntry;
    bufMgr->unPinPage(file, pageId, true);
    bufMgr->unPinPage(file, newPageId, true);
    if (pageId == rootPageNum) {
        updateRoot(pageId, newEntry);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitLeaf
// -----------------------------------------------------------------------------
/**
 * Split function to split a leaf node into two
 * It moves the records after the split index into a new node.
 *
 * @param node         the original given we will split from
 * @param leafPageId  the page ID of the splitting leaf
 * @param newEntry     the new entry which is a page key pair pushed up to after splitting
 * @param entry    the data entry given to perform insertion
*/
const void BTreeIndex::splitLeaf(LeafNodeInt *node, PageId leafPageId, PageKeyPair<int> *&newEntry,
        const RIDKeyPair<int> entry) {
    // Allocate a new leaf page
    Page *newPage;
    PageId newPageNum;
    bufMgr->allocPage(file, newPageNum, newPage);
    LeafNodeInt *newLeafNode = (LeafNodeInt *) newPage;

    // Set up the mid point
    int midPt = leafOccupancy / 2;
    // Check and adjust mid point
    if (leafOccupancy % 2 == 1 && entry.key > node->keyArray[midPt])
        midPt = midPt + 1;
    for (int i = midPt; i < leafOccupancy; i++) {
        newLeafNode->ridArray[i - midPt] = node->ridArray[i];
        newLeafNode->keyArray[i - midPt] = node->keyArray[i];

        node->ridArray[i].page_number = 0;
        node->keyArray[i] = 0;
    }

    // Performing leaf insertion
    if (entry.key > node->keyArray[midPt - 1])
        leafInsertion(newLeafNode, entry);
    else
        leafInsertion(node, entry);

    newLeafNode->rightSibPageNo =newPageNum;
    // Updating root after insertion
    newEntry = new PageKeyPair<int>();
    PageKeyPair<int> newKeyPair;
    newKeyPair.set(newPageNum, newLeafNode->keyArray[0]);
    newEntry = &newKeyPair;
    bufMgr->unPinPage(file, leafPageId, true);
    bufMgr->unPinPage(file, newPageNum, true);
    if (leafPageId == rootPageNum) {
        updateRoot(leafPageId, newEntry);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::updateRoot
// -----------------------------------------------------------------------------
/**
 * Update the root after splitting
 * This helper method create a new root
 *
 * @param firstPid   the first page ID in the root page
 * @param newEntry   the keyPair that is pushed up after splitting
*/
const void BTreeIndex::updateRoot(PageId firstPid, PageKeyPair<int> *newEntry) {
        // Alloc a new page for root
        PageId newRootPageId;
        Page *root;
        bufMgr->allocPage(file, newRootPageId, root);
        NonLeafNodeInt *newRoot = (NonLeafNodeInt *) root;

        // Set up the key and page numbers
        newRoot->level = initialRootPageNum == rootPageNum ? 1 : 0;
        newRoot->pageNoArray[0] = firstPid;
        newRoot->pageNoArray[1] = newEntry->pageNo;
        newRoot->keyArray[0] = newEntry->key;

        // Updating the index meta infromation
        Page *metaData;
        bufMgr->readPage(file, headerPageNum, metaData);
        IndexMetaInfo *metaPage = (IndexMetaInfo *) metaData;
        metaPage->rootPageNo = newRootPageId;
        rootPageNum = newRootPageId;

        // Unpin as soon as you can
        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, newRootPageId, true);
    }

// -----------------------------------------------------------------------------
// BTreeIndex::leafInsertion
// -----------------------------------------------------------------------------
/**
  * Inserts the given record ID pair into the leaf node given
  *
  * @param node    the leaf node given for insertion
  * @param entry   the entry of the record ID pair given for inserting
  */
const void BTreeIndex::leafInsertion(LeafNodeInt *node, RIDKeyPair<int> entry) {
    if (node->ridArray[0].page_number == 0) {
        // Set the record Id if leaf page empty
        node->ridArray[0] = entry.rid;
        node->keyArray[0] = entry.key;
    }
    else {
        size_t length = 0;
        int i = leafOccupancy - 1;
        while (i >= 0 && (node->ridArray[i].page_number == 0)) {
            i--;
            length++;
        }
        memmove(&node->keyArray[i + 1], &node->keyArray[i], length * sizeof(int));
        memmove(&node->ridArray[i + 1], &node->ridArray[i], length * sizeof(RecordId));

        // save the key and record id to the leaf node
        node->keyArray[i + 1] = entry.key;
        node->ridArray[i + 1] = entry.rid;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::nonLeafInsertion
// -----------------------------------------------------------------------------
/**
  * Inserts the given key page ID pair into the given leaf node given
  * 
  * @param node    the leaf node given for insertion
  * @param entry   the entry of the record ID pair given for inserting
  */
const void BTreeIndex::nonLeafInsertion(NonLeafNodeInt *node, PageKeyPair<int> *entry) {
    size_t length = 0;
    int i = nodeOccupancy;
    while (i >= 0 && (node->pageNoArray[i] == 0)) {
        i--;
        length++;
    }
    memmove(&node->keyArray[i], &node->keyArray[i - 1], length * sizeof(int));
    memmove(&node->pageNoArray[i + 1], &node->pageNoArray[i], length * sizeof(PageId));

    // store the key and page number to the node
    node->keyArray[i] = entry->key;
    node->pageNoArray[i + 1] = entry->pageNo;
}

// -----------------------------------------------------------------------------
// BTreeIndex::checkSatisfy
// -----------------------------------------------------------------------------
/**
 * Checking if the record ID satisfy with the value of rang and the pointer type,
 * and the operations.
 *
 * @param lowVal  Low value of range, pointer to integer / double / char string
 * @param lowOp   Low operator (GT/GTE)
 * @param highVal High value of range, pointer to integer / double / char string
 * @param highOp  High operator (LT/LTE)
 * @param val     value of the rid
 * @return true   If the rid satisfy
 */
const bool BTreeIndex::checkSatisfy(int lowVal, const Operator lowOp, int highVal, const Operator highOp, int val) {
    if (lowOp == GTE && highOp == LTE) {
        return val >= lowVal && val <= highVal;
    } else if (lowOp == GT && highOp == LTE) {
        return val > lowVal && val <= highVal;
    } else if (lowOp == GTE && highOp == LT) {
        return val >= lowVal && val < highVal;
    } else {
        return val > lowVal && val < highVal;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::findNext
// -----------------------------------------------------------------------------
/**
 * To find page Id of node that the key value should be at the next level
 *
 * @param node          the current node given
 * @param nextNodeNum   value for the page ID at the next level
 * @param val           the value of key given
*/
const void BTreeIndex::findNext(NonLeafNodeInt *node, PageId &nextNodeNum, int val) {
    int i = nodeOccupancy;
    while (i >= 0 && (node->pageNoArray[i] == 0)) {
        i--;
    }
    while (i > 0 && (node->keyArray[i - 1] >= val)) {
        i--;
    }
    nextNodeNum = node->pageNoArray[i];
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------
/**
 * Begin a filtered scan of the index.  For instance,
 * if the method is called using ("a",GT,"d",LTE) then we should seek all entries with a value
 * greater than "a" and less than or equal to "d".
 * If another scan is already executing, that needs to be ended here.
 * Set up all the variables for scan. Start from root to find out the leaf page that contains the
 * first RecordID that satisfies the scan parameters. Keep that page pinned in the buffer pool.
 *
 * @param lowValParm	Low value of range, pointer to integer / double / char string
 * @param lowOpParm		Low operator (GT/GTE)
 * @param highValParm	High value of range, pointer to integer / double / char string
 * @param highOpParm	High operator (LT/LTE)
 * @throws  BadOpcodesException      If lowOp and highOp do not contain one of their their expected values
 * @throws  BadScanrangeException    If lowVal > highval
 * @throws  NoSuchKeyFoundException  If there is no key in the B+ tree that satisfies the scan criteria.
 **/
const void BTreeIndex::startScan(const void *lowValParm,
        const Operator lowOpParm,
        const void *highValParm,
        const Operator highOpParm)
        {
        // Throw BadOpcodesExceptions
        if (lowOpParm != GT && lowOpParm != GTE) throw BadOpcodesException();
        if (highOpParm != LT && highOpParm != LTE) throw BadOpcodesException();

        lowOp = lowOpParm;
        highOp = highOpParm;
        lowValInt = *((int *) lowValParm);
        highValInt = *((int *) highValParm);
        if (lowValInt > highValInt)
            throw BadScanrangeException();

        // Check scanning
        if (scanExecuting)
            endScan();

        currentPageNum = rootPageNum;
        bufMgr->readPage(file, currentPageNum, currentPageData);

        // Whether found the leaf
        bool found = false;
        // Non leaf node case
        NonLeafNodeInt *node = (NonLeafNodeInt *) currentPageData;
        if (initialRootPageNum != rootPageNum) {
            while (!found) {
                // Check leaf
                node = (NonLeafNodeInt *) currentPageData;
                if (node->level == 1) {
                    found = true;
                }
                PageId nextPageNum;
                findNext(node, nextPageNum, lowValInt);
                // UnPin as soon as you can
                bufMgr->unPinPage(file, currentPageNum, false);
                // Turn to next
                currentPageNum = nextPageNum;
                bufMgr->readPage(file, currentPageNum, currentPageData);
            }
        }

        // Leaf node case
        found = false;
        LeafNodeInt *leafNode = (LeafNodeInt *) currentPageData;
        while (!found) {
            if (leafNode->ridArray[0].page_number == 0) {
                // Unpin as soon as you can
                bufMgr->unPinPage(file, currentPageNum, false);
                // No key satisfies the scan criteria since page number is 0
                throw NoSuchKeyFoundException();
            }

            // Whether inserted
            bool foundVal = false;
            for (int i = 0; i < leafOccupancy && !foundVal; i++) {
                int val = leafNode->keyArray[i];
                if (i < leafOccupancy - 1 and leafNode->ridArray[i + 1].page_number == 0) {
                    foundVal = true;
                }

                // Whether found the key satisfies the scan criteria
                if (checkSatisfy(lowValInt, lowOp, highValInt, highOp, val)) {
                    found = true;
                    nextEntry = i;
                    scanExecuting = true;
                    // Break out to continue since we find the fit
                    break;
                } else if (highOp == LT && val >= highValInt) {
                    // Unpin as soon as you can
                    bufMgr->unPinPage(file, currentPageNum, false);
                    throw NoSuchKeyFoundException();
                } else if (highOp == LTE && val > highValInt){
                    // Unpin as soon as you can
                    bufMgr->unPinPage(file, currentPageNum, false);
                    throw NoSuchKeyFoundException();
                }

                if ( foundVal || i == leafOccupancy - 1 ) {
                    bufMgr->unPinPage(file, currentPageNum, false);
                    // No key satisfies the scan criteria since right page number is 0
                    if (leafNode->rightSibPageNo == 0) {
                        throw NoSuchKeyFoundException();
                    }
                    // Turn to next
                    currentPageNum = leafNode->rightSibPageNo;
                    bufMgr->readPage(file, currentPageNum, currentPageData);
                }
            }
        }
    }


// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------
/**
  * Fetch the record id of the next index entry that matches the scan.
  * Return the next record from current page being scanned. If current page has been scanned to its entirety,
  * move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to
  * unpin any pages that are no longer required.
  * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
  * @throws ScanNotInitializedException If no scan has been initialized.
  * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
 **/
const void BTreeIndex::scanNext(RecordId& outRid) {
    // Throw ScanNotInitializedException
    if (!scanExecuting)
        throw ScanNotInitializedException();

    LeafNodeInt *node = (LeafNodeInt *) currentPageData;

    // outRid is the record ID of next record found
    outRid = node->ridArray[nextEntry];
    if (outRid.page_number == 0 || nextEntry == INTARRAYLEAFSIZE) {
        // UnPin as soon as you can
        bufMgr->unPinPage(file, currentPageNum, false);
        if (node->rightSibPageNo == Page::INVALID_NUMBER)
            throw IndexScanCompletedException();
        nextEntry = 0;
        currentPageNum = node->rightSibPageNo;
        bufMgr->readPage(file, currentPageNum, currentPageData);
        node = (LeafNodeInt *) currentPageData;
    }

    int val = node->keyArray[nextEntry];
    if (checkSatisfy(lowValInt, lowOp, highValInt, highOp, val))
        nextEntry++;
    else
        throw IndexScanCompletedException();
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
/**
  * Terminate the current scan. Unpin any pinned pages.
  * Reset scan specific variables.
  * @throws ScanNotInitializedException If no scan has been initialized.
  **/
const void BTreeIndex::endScan()
{
    if(!scanExecuting)
        throw ScanNotInitializedException();

    scanExecuting = false;
    bufMgr->unPinPage(file, currentPageNum, false); // Unpin
}

}
