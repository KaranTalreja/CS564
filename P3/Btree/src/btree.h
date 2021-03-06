/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#pragma once

#include <iostream>
#include <string>
#include "string.h"
#include <sstream>
#include <vector>

#include "types.h"
#include "page.h"
#include "file.h"
#include "buffer.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include <assert.h>

//#define DEBUG

#ifdef DEBUG
#include <iostream>
//using namespace std;
#endif

namespace badgerdb
{

/**
 * @brief Datatype enumeration type.
 */
enum Datatype
{
	INTEGER = 0,
	DOUBLE = 1,
	STRING = 2
};

/**
 * @brief Scan operations enumeration. Passed to BTreeIndex::startScan() method.
 */
enum Operator
{ 
	LT, 	/* Less Than */
	LTE,	/* Less Than or Equal to */
	GTE,	/* Greater Than or Equal to */
	GT		/* Greater Than */
};

/**
 * @brief Size of String key.
 */
const  int STRINGSIZE = 10;

/**
 * @brief Number of key slots in B+Tree leaf for INTEGER key.
 */
//                                                  sibling ptr             key               rid
const  int INTARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) ) / ( sizeof( int ) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree leaf for DOUBLE key.
 */
//                                                     sibling ptr               key               rid
const  int DOUBLEARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) ) / ( sizeof( double ) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree leaf for STRING key.
 */
//                                                    sibling ptr           key                      rid
const  int STRINGARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) ) / ( 10 * sizeof(char) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree non-leaf for INTEGER key.
 */
//                                                     level     extra pageNo                  key       pageNo
const  int INTARRAYNONLEAFSIZE = ( Page::SIZE - sizeof( int ) - sizeof( PageId ) ) / ( sizeof( int ) + sizeof( PageId ) );

/**
 * @brief Number of key slots in B+Tree leaf for DOUBLE key.
 */
//                                                        level        extra pageNo                 key            pageNo   -1 due to structure padding
const  int DOUBLEARRAYNONLEAFSIZE = (( Page::SIZE - sizeof( int ) - sizeof( PageId ) ) / ( sizeof( double ) + sizeof( PageId ) )) - 1;

/**
 * @brief Number of key slots in B+Tree leaf for STRING key.
 */
//                                                        level        extra pageNo             key                   pageNo
const  int STRINGARRAYNONLEAFSIZE = ( Page::SIZE - sizeof( int ) - sizeof( PageId ) ) / ( 10 * sizeof(char) + sizeof( PageId ) );

/**
 * @brief Structure to store a key-rid pair. It is used to pass the pair to functions that 
 * add to or make changes to the leaf node pages of the tree. Is templated for the key member.
 */
template <class T>
class RIDKeyPair{
public:
	RecordId rid;
	T key;
	void set( RecordId r, T k)
	{
		rid = r;
		key = k;
	}
};

/**
 * @brief Structure to store a key page pair which is used to pass the key and page to functions that make 
 * any modifications to the non leaf pages of the tree.
*/
template <class T>
class PageKeyPair{
public:
	PageId pageNo;
	T key;
	void set( int p, T k)
	{
		pageNo = p;
		key = k;
	}
};

/**
 * @brief Overloaded operator to compare the key values of two rid-key pairs
 * and if they are the same compares to see if the first pair has
 * a smaller rid.pageNo value.
*/
template <class T>
bool operator<( const RIDKeyPair<T>& r1, const RIDKeyPair<T>& r2 )
{
	if( r1.key != r2.key )
		return r1.key < r2.key;
	else
		return r1.rid.page_number < r2.rid.page_number;
}

/**
 * @brief The meta page, which holds metadata for Index file, is always first page of the btree index file and is cast
 * to the following structure to store or retrieve information from it.
 * Contains the relation name for which the index is created, the byte offset
 * of the key value on which the index is made, the type of the key and the page no
 * of the root page. Root page starts as page 2 but since a split can occur
 * at the root the root page may get moved up and get a new page no.
*/
struct IndexMetaInfo{
  /**
   * Name of base relation.
   */
	char relationName[20];

  /**
   * Offset of attribute, over which index is built, inside the record stored in pages.
   */
	int attrByteOffset;

  /**
   * Type of the attribute over which index is built.
   */
	Datatype attrType;

  /**
   * Page number of root page of the B+ Tree inside the file index file.
   */
	PageId rootPageNo;
};

/*
Each node is a page, so once we read the page in we just cast the pointer to the page to this struct and use it to access the parts
These structures basically are the format in which the information is stored in the pages for the index file depending on what kind of 
node they are. The level memeber of each non leaf structure seen below is set to 1 if the nodes 
at this level are just above the leaf nodes. Otherwise set to 0.
*/

/**
 * @brief Structure for all non-leaf nodes when the key is of INTEGER type.
*/
struct NonLeafNodeInt{
  /**
   * Level of the node in the tree.
   */
	int level;

  /**
   * Stores keys.
   */
	int keyArray[ INTARRAYNONLEAFSIZE ];

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf nodes in the tree.
   */
	PageId pageNoArray[ INTARRAYNONLEAFSIZE + 1 ];
};

/**
 * @brief Structure for all non-leaf nodes when the key is of DOUBLE type.
*/
struct NonLeafNodeDouble{
  /**
   * Level of the node in the tree.
   */
	int level;

  /**
   * Stores keys.
   */
	double keyArray[ DOUBLEARRAYNONLEAFSIZE ];

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf nodes in the tree.
   */
	PageId pageNoArray[ DOUBLEARRAYNONLEAFSIZE + 1 ];
};

/**
 * @brief Structure for all non-leaf nodes when the key is of STRING type.
*/
struct NonLeafNodeString{
  /**
   * Level of the node in the tree.
   */
	int level;

  /**
   * Stores keys.
   */
	char keyArray[ STRINGARRAYNONLEAFSIZE ][ STRINGSIZE ];

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf nodes in the tree.
   */
	PageId pageNoArray[ STRINGARRAYNONLEAFSIZE + 1 ];
};

/**
 * @brief Structure for all leaf nodes when the key is of INTEGER type.
*/
struct LeafNodeInt{
  /**
   * Stores keys.
   */
	int keyArray[ INTARRAYLEAFSIZE ];

  /**
   * Stores RecordIds.
   */
	RecordId ridArray[ INTARRAYLEAFSIZE ];

  /**
   * Page number of the leaf on the right side.
	 * This linking of leaves allows to easily move from one leaf to the next leaf during index scan.
   */
	PageId rightSibPageNo;
};

/**
 * @brief Structure for all leaf nodes when the key is of DOUBLE type.
*/
struct LeafNodeDouble{
  /**
   * Stores keys.
   */
	double keyArray[ DOUBLEARRAYLEAFSIZE ];

  /**
   * Stores RecordIds.
   */
	RecordId ridArray[ DOUBLEARRAYLEAFSIZE ];

  /**
   * Page number of the leaf on the right side.
	 * This linking of leaves allows to easily move from one leaf to the next leaf during index scan.
   */
	PageId rightSibPageNo;
};

/**
 * @brief Structure for all leaf nodes when the key is of STRING type.
*/
struct LeafNodeString{
  /**
   * Stores keys.
   */
	char keyArray[ STRINGARRAYLEAFSIZE ][ STRINGSIZE ];

  /**
   * Stores RecordIds.
   */
	RecordId ridArray[ STRINGARRAYLEAFSIZE ];

  /**
   * Page number of the leaf on the right side.
	 * This linking of leaves allows to easily move from one leaf to the next leaf during index scan.
   */
	PageId rightSibPageNo;
};

/**
 * @brief BTreeIndex class. It implements a B+ Tree index on a single attribute of a
 * relation. This index supports only one scan at a time.
*/
class BTreeIndex {

 private:

  /**
   * File object for the index file.
   */
	File		*file;

  /**
   * Buffer Manager Instance.
   */
	BufMgr	*bufMgr;

  /**
   * Page number of meta page.
   */
	PageId	headerPageNum;

  /**
   * page number of root page of B+ tree inside index file.
   */
	PageId	rootPageNum;

  /**
   * Datatype of attribute over which index is built.
   */
	Datatype	attributeType;

  /**
   * Offset of attribute, over which index is built, inside records. 
   */
	int 		attrByteOffset;

  /**
   * Number of keys in leaf node, depending upon the type of key.
   */
	int		leafOccupancy;

  /**
   * Number of keys in non-leaf node, depending upon the type of key.
   */
	int		nodeOccupancy;


	// MEMBERS SPECIFIC TO SCANNING

  /**
   * True if an index scan has been started.
   */
	bool		scanExecuting;

  /**
   * Index of next entry to be scanned in current leaf being scanned.
   */
	int		nextEntry;

  /**
   * Page number of current page being scanned.
   */
	PageId	currentPageNum;

  /**
   * Current Page being scanned.
   */
	Page		*currentPageData;

  /**
   * Low INTEGER value for scan.
   */
	int		lowValInt;

  /**
   * Low DOUBLE value for scan.
   */
	double	lowValDouble;

  /**
   * Low STRING value for scan.
   */
	std::string	lowValString;

  /**
   * High INTEGER value for scan.
   */
	int		highValInt;

  /**
   * High DOUBLE value for scan.
   */
	double	highValDouble;

  /**
   * High STRING value for scan.
   */
	std::string highValString;
	
  /**
   * Low Operator. Can only be GT(>) or GTE(>=).
   */
	Operator	lowOp;

  /**
   * High Operator. Can only be LT(<) or LTE(<=).
   */
	Operator	highOp;

	
 public:

  /**
   * BTreeIndex Constructor. 
	 * Check to see if the corresponding index file exists. If so, open the file.
	 * If not, create it and insert entries for every tuple in the base relation using FileScan class.
   *
   * @param relationName        Name of file.
   * @param outIndexName        Return the name of index file.
   * @param bufMgrIn						Buffer Manager Instance
   * @param attrByteOffset			Offset of attribute, over which index is to be built, in the record
   * @param attrType						Datatype of attribute over which index is built
   * @throws  BadIndexInfoException     If the index file already exists for the corresponding attribute, but values in metapage(relationName, attribute byte offset, attribute type etc.) do not match with values received through constructor parameters.
   */
	BTreeIndex(const std::string & relationName, std::string & outIndexName,
						BufMgr *bufMgrIn,	const int attrByteOffset,	const Datatype attrType);
	

  /**
   * BTreeIndex Destructor. 
	 * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
	 * and delete file instance thereby closing the index file.
	 * Destructor should not throw any exceptions. All exceptions should be caught in here itself. 
	 * */
	~BTreeIndex();


  /**
	 * Insert a new entry using the pair <value,rid>. 
	 * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
	 * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
	 * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
	 * Make sure to unpin pages as soon as you can.
   * @param key			Key to insert, pointer to integer/double/char string
   * @param rid			Record ID of a record whose entry is getting inserted into the index.
	**/
	const void insertEntry(const void* key, const RecordId rid);

	const bool deleteEntry(const void* key);

  /**
	 * Begin a filtered scan of the index.  For instance, if the method is called 
	 * using ("a",GT,"d",LTE) then we should seek all entries with a value 
	 * greater than "a" and less than or equal to "d".
	 * If another scan is already executing, that needs to be ended here.
	 * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
	 * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
   * @param lowVal	Low value of range, pointer to integer / double / char string
   * @param lowOp		Low operator (GT/GTE)
   * @param highVal	High value of range, pointer to integer / double / char string
   * @param highOp	High operator (LT/LTE)
   * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values 
   * @throws  BadScanrangeException If lowVal > highval
	 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
	**/
	const void startScan(const void* lowVal, const Operator lowOp, const void* highVal, const Operator highOp);


  /**
	 * Fetch the record id of the next index entry that matches the scan.
	 * Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
   * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
	 * @throws ScanNotInitializedException If no scan has been initialized.
	 * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
	**/
	const void scanNext(RecordId& outRid);  // returned record id


  /**
	 * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
	 * @throws ScanNotInitializedException If no scan has been initialized.
	**/
	const void endScan();

	/**
	 * @brief Static class which needs to be defined for all data types for which Btree indexing is available.
	 * This class would define certain key things required by the Btree implementation for each data type.
	 * This class is a default template parameter for all Btree routines.
	 */
  template <typename keyType> class keyTraits;

private:
  ///@brief Templated routine with keyType as template parameter for call from startScan routine.
	template <typename keyType, typename traits=keyTraits<keyType> >
	const void startScanTemplate(const void* lowVal, const void* highVal);

	///@brief Templated routine with keyType as template parameter for call from scanNext routine.
  template <typename keyType, typename traits=keyTraits<keyType> >
  const void scanNextTemplate(RecordId& outRid);

  ///@brief Templated routine with keyType as template parameter for call from insertKeyTemplate routine.
	template <typename keyType, typename traits=keyTraits<keyType> >
  void getPageNoAndOffsetOfKeyInsert(const void* key, Page* rootPage, PageId& pageNo, int& insertAt, int& endOfRecordsOffset, PageId& lastPageNo, bool insert = true);

	///@brief Templated routine with keyType as template parameter for call from constructor.
	template <typename keyType, typename traits=keyTraits<keyType> >
	void createRoot(Page*& rootPage);

	///@brief Templated routine with keyType as template parameter for call from insertKey routine.
  template <typename keyType, typename traits=keyTraits<keyType> >
  const void insertKeyTemplate(const void* key, const RecordId rid);

  ///@brief Templated routine with keyType as template parameter for call from deleteKey routine.
  template <typename keyType, typename traits=keyTraits<keyType> >
  const bool deleteKeyTemplate(const void* key);
  
  ///@brief Templated routine with keyType as template parameter for getting sibling page for a page in BTree.
  template<typename keyType, typename traits=keyTraits<keyType> >
  PageId getSiblingPage (Page* currPage, std::vector<std::pair<int,PageId>>& pathOfTraversal, bool dataPage, bool right, PageId parentPageId);
  
  ///@brief Templated routine with keyType as template parameter for getting the occupancy of a leaf/nonleaf page
  template<typename keyType, typename traits=keyTraits<keyType> >
  int getOccupancy(Page* currPage, bool dataPage);
  
  ///@brief Templated routine with keyType as template parameter for deleting entry in a leaf page
  template<typename keyType, typename traits=keyTraits<keyType>>
  void deleteEntryInLeaf(Page* leafPage, int startLoc, int endLoc);
  
  ///@brief Templated routine with keyType as template parameter for getting left and right sibling page occupancy
  template<typename keyType, typename traits=keyTraits<keyType>>
  void getLeftRightSiblingOccupancy (Page* currPage, PageId& leftSibling, Page*& leftSib, int& leftOccupancy, PageId& rightSibling, Page*& rightSib, int& rightOccupancy, std::vector<std::pair<int,PageId>>& pathOfTraversal, bool dataPage, PageId parentPageId);
  
  ///@brief Templated routine with keyType as template parameter for deleting entry in non-leaf page
  template<typename keyType, typename traits=keyTraits<keyType>>
  void deleteEntryInNonLeaf(Page* nonLeafPage, int startLoc, int endLoc);
  
  ///@brief Templated routine with keyType as template parameter for shifting entry right from left page.
  template<typename keyType, typename traits=keyTraits<keyType>>
  void shiftRightPage (Page* leftSib, Page* page, int startLoc, int endLoc, int leftOccupancy, bool dataPage);

  ///@brief Templated routine with keyType as template parameter for shifting entry left from right page.
  template<typename keyType, typename traits=keyTraits<keyType>>
  void shiftLeftPage (Page* rightSib, Page* page, int startLoc, int endLoc, int rightOccupancy, bool dataPage);

  ///@brief Templated routine with keyType as template parameter for checking whether the BTree structure is valid.
  template<typename keyType, typename traits=keyTraits<keyType>>
  bool isStructureValid();
};

// This is the structure for tuples in the base relation

typedef struct tuple {
	int i;
	double d;
	char s[64];
} RECORD;

///@brief Partial template specialization for static class keyTraits for int data type specialization.
template<>
class BTreeIndex::keyTraits<int> {
public:
   typedef LeafNodeInt leafType;
   typedef NonLeafNodeInt nonLeafType;
   static const int LEAFSIZE = INTARRAYLEAFSIZE;
   static const int NONLEAFSIZE = INTARRAYNONLEAFSIZE;
   static void setScanBounds(BTreeIndex* index, const void* lowValParm, const void* highValParm) {
     index->lowValInt = *reinterpret_cast<int*>(const_cast<void*>(lowValParm));
     index->highValInt = *reinterpret_cast<int*>(const_cast<void*>(highValParm));
     if (index->lowValInt > index->highValInt) throw BadScanrangeException();
   }

   static inline int getLowBound(BTreeIndex* index) {
     return index->lowValInt;
   }

   static inline int getUpperBound(BTreeIndex* index) {
     return index->highValInt;
   }

   static bool less(int a, int b) {
     return a < b;
   }

   static bool great(int a, int b) {
     return a > b;
   }

   static bool lessE(int a, int b) {
     return a <= b;
   }

   static bool greatE(int a, int b) {
     return a >= b;
   }

   static bool equal(int a, int b) {
     return a == b;
   }

   static void assign(int& a, int b) {
        a = b;
   }

   static int getKeyValue(const void* key) {
      return *reinterpret_cast<int*>(const_cast<void*>(key));
   }
};

///@brief Partial template specialization for static class keyTraits for double data type specialization.
template<>
class BTreeIndex::keyTraits<double> {
public:
   typedef LeafNodeDouble leafType;
   typedef NonLeafNodeDouble nonLeafType;
   static const int LEAFSIZE = DOUBLEARRAYLEAFSIZE;
   static const int NONLEAFSIZE = DOUBLEARRAYNONLEAFSIZE;
   static void setScanBounds(BTreeIndex* index, const void* lowValParm, const void* highValParm) {
     index->lowValDouble = *reinterpret_cast<double*>(const_cast<void*>(lowValParm));
     index->highValDouble = *reinterpret_cast<double*>(const_cast<void*>(highValParm));
     if (index->lowValDouble > index->highValDouble) throw BadScanrangeException();
   }

   static inline double getLowBound(BTreeIndex* index) {
     return index->lowValDouble;
   }

   static inline double getUpperBound(BTreeIndex* index) {
     return index->highValDouble;
   }

   static bool less(double a, double b) {
     return a < b;
   }

   static bool great(double a, double b) {
     return a > b;
   }

   static bool lessE(double a, double b) {
     return a <= b;
   }

   static bool greatE(double a, double b) {
     return a >= b;
   }

   static bool equal(double a, double b) {
     return a == b;
   }

   static void assign(double& a, double b) {
        a = b;
   }

   static double getKeyValue(const void* key) {
      return *reinterpret_cast<double*>(const_cast<void*>(key));
   }
};

///@brief Partial template specialization for static class keyTraits for char* data type specialization.
template<>
class BTreeIndex::keyTraits<char*> {
public:
   typedef LeafNodeString leafType;
   typedef NonLeafNodeString nonLeafType;
   static const int LEAFSIZE = STRINGARRAYLEAFSIZE;
   static const int NONLEAFSIZE = STRINGARRAYNONLEAFSIZE;
   static void setScanBounds(BTreeIndex* index, const void* lowValParm, const void* highValParm) {
     index->lowValString = std::string(reinterpret_cast<char*>(const_cast<void*>(lowValParm)));
     index->highValString = std::string(reinterpret_cast<char*>(const_cast<void*>(highValParm)));
     if (index->lowValString > index->highValString) throw BadScanrangeException();
   }

   static inline std::string getLowBound(BTreeIndex* index) {
     return index->lowValString;
   }

   static inline std::string getUpperBound(BTreeIndex* index) {
     return index->highValString;
   }

   static bool less(char* a, char* b) {
     return strncmp(a,b,STRINGSIZE) < 0 ? true : false;
   }

   static bool great(char* a, std::string b) {
     return strncmp(a,b.c_str(),STRINGSIZE) > 0 ? true : false;
   }

   static bool great(char* a, char* b) {
     return strncmp(a,b,STRINGSIZE) > 0 ? true : false;
   }

   static bool lessE(char* a, char* b) {
     return strncmp(a,b,STRINGSIZE) <= 0 ? true : false;
   }

   static bool greatE(char* a, std::string b) {
     return strncmp(a,b.c_str(),STRINGSIZE) >= 0 ? true : false;
   }

   static bool greatE(char* a, char* b) {
     return strncmp(a,b,STRINGSIZE) >= 0 ? true : false;
   }

   static bool equal(char* a, std::string b) {
     return strncmp(a,b.c_str(),STRINGSIZE) == 0 ? true : false;
   }

   static bool equal(char* a, char* b) {
     return strncmp(a,b,STRINGSIZE) == 0 ? true : false;
   }

   static void assign(char* a, char* b) {
      if (a == NULL) return;
      if (b != NULL) strncpy(a,b,STRINGSIZE);
      else strcpy(a,""); 
   }

   static char* getKeyValue(const void* key) {
     return reinterpret_cast<char*>(const_cast<void*>(key));
   }
};

/**
 * @param rootPage  The root page in which root page is to be created.
 */
template<typename keyType, typename traits>
void BTreeIndex::createRoot(Page*& rootPage) {
  typedef typename traits::nonLeafType nonLeafType;
  nonLeafType rootData;
  memset(&rootData, 0, sizeof(nonLeafType));
  rootData.level = 1;
  memcpy(rootPage, &rootData, sizeof(nonLeafType));
  this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
}

template<typename keyType, typename traits>
void BTreeIndex::getPageNoAndOffsetOfKeyInsert(const void* key, Page* rootPage, PageId& pageNo, int& insertAt, int& endOfRecordsOffset, PageId& lastPageNo, bool insert)
{
  typedef typename traits::leafType leafType;
  typedef typename traits::nonLeafType nonLeafType;
  int i = 0, depth = 1;
  keyType keyValue = traits::getKeyValue(key);
  nonLeafType* rootData = reinterpret_cast<nonLeafType*>(rootPage);
  nonLeafType* currPage = rootData;
  // <going to pade index , coming from page id>
  std::vector<std::pair<int,PageId>> pathOfTraversal;
  PageId lastPage = this->rootPageNum;
  Page* tempPage;
  while (depth < rootData->level) {
    if (traits::less(keyValue,currPage->keyArray[0])) {
      // Case smaller than all keys
      i = 0;
      pathOfTraversal.push_back(std::pair<int,PageId>(i, lastPage));
    } else {
      // invariant page[i] contains keys >= key[i-1]
      // invariant page[i] contains keys < key [i]
      for (i = 0; i < traits::NONLEAFSIZE; i++) {
        if (currPage->pageNoArray[i+1] == Page::INVALID_NUMBER) {
          pathOfTraversal.push_back(std::pair<int,PageId>(i, lastPage));
          break;
        }
        /* 1st page contains keys greater than key[0] so if keyValue is greater than key[1]:
         * the key must lie in page[2] or ahead. Since page[2] contains keys greater than key[1] */
        if (traits::lessE(currPage->keyArray[i],keyValue)) {
          // If the next page is not invalid, it might contain the key, so continue.
#ifdef DEBUG
          // keys all smaller than keyArray[i] should lie in this page so it should be valid
          assert(currPage->pageNoArray[i] != Page::INVALID_NUMBER);
          // keys all greater than equal to keyArray[i] should lie in page[i+1] or ahead.
          assert(currPage->pageNoArray[i+1] != Page::INVALID_NUMBER);
#endif
            continue; // means if this was the last page in the node, we need to add to this page only otherwise continue
        }
        pathOfTraversal.push_back(std::pair<int,PageId>(i, lastPage));
        break;
      }
    }
    if (i == traits::NONLEAFSIZE) {
      pathOfTraversal.push_back(std::pair<int,PageId>(i, lastPage));
    }
    // TODO karantalreja : if i == traits::NONLEAFSIZE then need to split page
    this->bufMgr->unPinPage(this->file, lastPage, false);
    this->bufMgr->readPage(this->file, currPage->pageNoArray[i], tempPage);
    lastPage = currPage->pageNoArray[i];
    currPage = reinterpret_cast<nonLeafType*>(tempPage);
    depth++;
  }
  pageNo = lastPage;
  i = 0;
  insertAt = traits::LEAFSIZE;
  leafType* dataPage = reinterpret_cast<leafType*>(currPage);
  for (i = 0; i < traits::LEAFSIZE; i++) {
    if (dataPage->ridArray[i].page_number == Page::INVALID_NUMBER) {
      if (insertAt == traits::LEAFSIZE) insertAt = i;
      break;
    }
    if (traits::great(keyValue,dataPage->keyArray[i])) continue;
    if (insertAt == traits::LEAFSIZE) {
      insertAt = i;
      if (insert == false) break;
    }
  }
  bool done = false;
  if (i == traits::LEAFSIZE) {
    // split data page
    Page* greaterKey;
    int medianIdx = traits::LEAFSIZE/2;
    PageId GparentPageId;
    int Goffset;
    nonLeafType* GparentData;
    int k=0;
    while (pathOfTraversal.size() >= 1) {
      PageId parentPageId;
      int offset;
      Page* parentPage;
      nonLeafType* parentData;
      parentPageId = pathOfTraversal.back().second;
      offset = pathOfTraversal.back().first;  // The page idx in parent pageArray in which the key wants to go.
      pathOfTraversal.pop_back();
      this->bufMgr->readPage(this->file, parentPageId, parentPage);
      parentData = reinterpret_cast<nonLeafType*>(parentPage);
      if (done == false) {
        GparentPageId = parentPageId;
        Goffset = offset;
        GparentData = parentData;
      }
      for (k = offset; k <= traits::NONLEAFSIZE; k++) {
        if (parentData->pageNoArray[k] == Page::INVALID_NUMBER) break;
      }

      // Split parent page
      if (k == traits::NONLEAFSIZE+1) {
        Page* greaterParentPage;
        int medianIdxParent = traits::NONLEAFSIZE/2;
        nonLeafType newRootData;
        Page* newRoot;
        int parentParentOffset = 0;
        PageId parentParentPageId;
        if (pathOfTraversal.empty()) {
          this->bufMgr->allocPage(this->file, this->rootPageNum, newRoot);
          parentParentPageId = this->rootPageNum;
          memset(&newRootData, 0, sizeof(nonLeafType));
          newRootData.level = parentData->level+1;
          newRootData.pageNoArray[0] = parentPageId;
        } else {
          parentParentPageId = pathOfTraversal.back().second;
          this->bufMgr->readPage(this->file, parentParentPageId, newRoot);
          newRootData = *reinterpret_cast<nonLeafType*>(newRoot);
          parentParentOffset = pathOfTraversal.back().first;
        }
        for (k = parentParentOffset; k <= traits::NONLEAFSIZE; k++) {
          if (newRootData.pageNoArray[k] == Page::INVALID_NUMBER) break;
        }
        for (; k > parentParentOffset; k--) {
          if (k-1 >= 0) newRootData.pageNoArray[k] = newRootData.pageNoArray[k-1];
          if (k-2 >= 0) traits::assign(newRootData.keyArray[k-1],newRootData.keyArray[k-2]);
        }
#ifdef DEBUG
        assert(newRootData.pageNoArray[parentParentOffset+1] == Page::INVALID_NUMBER || newRootData.pageNoArray[parentParentOffset] == newRootData.pageNoArray[parentParentOffset+1]);
#endif
        this->bufMgr->allocPage(this->file, newRootData.pageNoArray[parentParentOffset+1], greaterParentPage);
        traits::assign(newRootData.keyArray[parentParentOffset], parentData->keyArray[medianIdxParent]);

        nonLeafType dataPageRight;
        memset(&dataPageRight, 0, sizeof(nonLeafType));
        dataPageRight.level = parentData->level;
        for (int i = medianIdxParent+1, j = 0; i < traits::NONLEAFSIZE; i++,j++) {
          traits::assign(dataPageRight.keyArray[j], parentData->keyArray[i]);
          dataPageRight.pageNoArray[j+1] = parentData->pageNoArray[i+1];
          traits::assign(parentData->keyArray[i], 0);
          parentData->pageNoArray[i+1] = Page::INVALID_NUMBER;
        }

        dataPageRight.pageNoArray[0] = parentData->pageNoArray[medianIdxParent+1];
        parentData->pageNoArray[medianIdxParent+1] = Page::INVALID_NUMBER;
        traits::assign(parentData->keyArray[medianIdxParent],0);

        if (done == false) {
          if (traits::greatE(keyValue,newRootData.keyArray[parentParentOffset])) {
            GparentData = reinterpret_cast<nonLeafType*>(greaterParentPage);
            Goffset = offset - medianIdxParent - 1;
            i = Goffset - 1;
            GparentPageId = newRootData.pageNoArray[parentParentOffset+1];
            done = true;
          } else {
            GparentData = parentData;
            Goffset = offset;
            GparentPageId = parentPageId;
            done = true;
          }
        }

        memcpy(newRoot, &newRootData, sizeof(nonLeafType));
        memcpy(greaterParentPage, &dataPageRight, sizeof(nonLeafType));

        this->bufMgr->unPinPage(this->file, parentParentPageId, true);
        if (traits::greatE(keyValue, newRootData.keyArray[parentParentOffset])) {
          this->bufMgr->unPinPage(this->file, newRootData.pageNoArray[parentParentOffset], true);
          if (newRootData.level >= 4)
            this->bufMgr->unPinPage(this->file, newRootData.pageNoArray[parentParentOffset+1], true);
        } else {
          this->bufMgr->unPinPage(this->file, newRootData.pageNoArray[parentParentOffset+1], true);
          if (newRootData.level >= 4)
            this->bufMgr->unPinPage(this->file, newRootData.pageNoArray[parentParentOffset], true);
        }
      } else {
        if (GparentPageId != parentPageId)
          this->bufMgr->unPinPage(this->file, parentPageId, true);
        break;
      }
    }
    PageId parentPageId = GparentPageId;
    int offset = Goffset;
    nonLeafType* parentData = GparentData;

    for (k = offset; k <= traits::NONLEAFSIZE; k++) {
      if (parentData->pageNoArray[k] == Page::INVALID_NUMBER) break;
    }
#ifdef DEBUG
    assert(k != traits::NONLEAFSIZE+1);
#endif
    for (; k > offset; k--) {
      if (k-1 >= 0) parentData->pageNoArray[k] = parentData->pageNoArray[k-1];
      if (k-2 >= 0) traits::assign(parentData->keyArray[k-1], parentData->keyArray[k-2]);
    }
    traits::assign(parentData->keyArray[offset],dataPage->keyArray[medianIdx]);
#ifdef DEBUG
    assert(offset == 0 || traits::less(parentData->keyArray[offset-1],parentData->keyArray[offset]));
    if (offset+2 < traits::NONLEAFSIZE && parentData->pageNoArray[offset+2] != Page::INVALID_NUMBER)
      assert(traits::great(parentData->keyArray[offset+1],parentData->keyArray[offset]));

    // As insert mode, the above loop should have copied this value to the next slot or
    // if its the first empty slot its value should be Page::INVALID_NUMBER
    assert(parentData->pageNoArray[offset+1] == Page::INVALID_NUMBER || parentData->pageNoArray[offset] == parentData->pageNoArray[offset+1]);
#endif
    this->bufMgr->allocPage(this->file, parentData->pageNoArray[offset+1], greaterKey);
    leafType dataPageRight;
    memset(&dataPageRight, 0, sizeof(leafType));
    dataPageRight.rightSibPageNo = dataPage->rightSibPageNo;
    dataPage->rightSibPageNo = parentData->pageNoArray[offset+1];
#ifdef DEBUG
    assert(insertAt == 0 || traits::less(dataPage->keyArray[insertAt-1],keyValue));
    assert(insertAt == traits::LEAFSIZE || traits::great(dataPage->keyArray[insertAt],keyValue));
#endif
    if (traits::great(keyValue,dataPage->keyArray[medianIdx])) {
      insertAt -= medianIdx;
      lastPageNo = pageNo;
      pageNo = parentData->pageNoArray[offset+1];
      endOfRecordsOffset = (traits::LEAFSIZE%2) ? medianIdx+1 : medianIdx;
    } else {
      lastPageNo = pageNo;
      pageNo = lastPage;
      endOfRecordsOffset = medianIdx;
    }
    for (int i = medianIdx, j = 0; i < traits::LEAFSIZE; i++,j++) {
      traits::assign(dataPageRight.keyArray[j],dataPage->keyArray[i]);
      dataPageRight.ridArray[j] = dataPage->ridArray[i];
      traits::assign(dataPage->keyArray[i],0);
      dataPage->ridArray[i].page_number = Page::INVALID_NUMBER;
      dataPage->ridArray[i].slot_number = 0;
    }
    memcpy(greaterKey, &dataPageRight, sizeof(leafType));
#ifdef DEBUG
    if (traits::great(keyValue,dataPageRight.keyArray[0])) {
      assert(insertAt == 0 || traits::less(dataPageRight.keyArray[insertAt-1], keyValue));
      assert(insertAt == traits::LEAFSIZE || insertAt == endOfRecordsOffset || traits::great(dataPageRight.keyArray[insertAt],keyValue));
    } else {
      assert(insertAt == 0 || traits::less(dataPage->keyArray[insertAt-1], keyValue));
      assert(insertAt == traits::LEAFSIZE || insertAt == endOfRecordsOffset || traits::great(dataPage->keyArray[insertAt],keyValue));
    }
#endif
    this->bufMgr->unPinPage(this->file, lastPage, true);
    this->bufMgr->unPinPage(this->file, parentPageId, true);
    this->bufMgr->unPinPage(this->file, parentData->pageNoArray[offset+1], true);
  } else {
    this->bufMgr->unPinPage(this->file, lastPage, false);
    endOfRecordsOffset = i;
    lastPageNo = pageNo;
  }
  #ifdef DEBUG
  assert(insertAt >= 0);
  assert(insertAt <= endOfRecordsOffset);
  assert(endOfRecordsOffset <= traits::LEAFSIZE);
  #endif
  return;
}


template <typename keyType, class traits>
const void BTreeIndex::scanNextTemplate(RecordId& outRid) {
  if (this->currentPageData == NULL) throw IndexScanCompletedException();
  typedef typename traits::leafType leafType;
  leafType* dataPage = reinterpret_cast<leafType*>(this->currentPageData);
  if (this->highOp == LT && traits::greatE(dataPage->keyArray[this->nextEntry],traits::getUpperBound(this))) {
    this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
    throw IndexScanCompletedException();
  }
  if (this->highOp == LTE && traits::great(dataPage->keyArray[this->nextEntry],traits::getUpperBound(this))) {
    this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
    throw IndexScanCompletedException();
  }
  outRid = dataPage->ridArray[this->nextEntry];
  #ifdef DEBUG
  assert(outRid.page_number != Page::INVALID_NUMBER);
  assert(outRid.slot_number != 0);
  #endif
  if (this->nextEntry + 1 == traits::LEAFSIZE || dataPage->ridArray[this->nextEntry+1].page_number == Page::INVALID_NUMBER) {
    this->nextEntry = 0;
    this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
    this->currentPageNum = dataPage->rightSibPageNo;
    if (this->currentPageNum == Page::INVALID_NUMBER) {
      this->currentPageData = NULL;
    } else this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
  } else this->nextEntry++;
}

template <typename keyType, class traits>
const void BTreeIndex::startScanTemplate(const void* lowVal, const void* highVal) {
  traits::setScanBounds(this, lowVal, highVal);
  typedef typename traits::leafType leafType;
  Page* rootPage;
  this->bufMgr->readPage(this->file, this->rootPageNum, rootPage);
  int insertAt, endOfRecordsOffset;
  PageId dataPageNum, dataPageNumPrev;
  this->getPageNoAndOffsetOfKeyInsert<keyType>(lowVal, rootPage, dataPageNum, insertAt, endOfRecordsOffset, dataPageNumPrev, false);
  if (dataPageNumPrev == dataPageNum) { //TODO karantalreja : Handle the non equal case
    this->currentPageNum = dataPageNum;
    this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
    this->nextEntry = insertAt;
    leafType* dataPage = reinterpret_cast<leafType*>(this->currentPageData);
    if (dataPage->ridArray[this->nextEntry].page_number == Page::INVALID_NUMBER) {
      if (Page::INVALID_NUMBER != dataPage->rightSibPageNo) {
        this->nextEntry = 0;
        this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
        this->currentPageNum = dataPage->rightSibPageNo;
        if (this->currentPageNum == Page::INVALID_NUMBER) {
          this->currentPageData = NULL;
          throw NoSuchKeyFoundException();
        } else this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
      } else {
        this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
        throw NoSuchKeyFoundException();
      }
    }
    if (this->lowOp == GT) {
      if (traits::equal(dataPage->keyArray[this->nextEntry],traits::getLowBound(this))) {
        if (this->nextEntry + 1 == traits::LEAFSIZE || dataPage->ridArray[this->nextEntry+1].page_number == Page::INVALID_NUMBER) {
          this->nextEntry = 0;
          this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
          this->currentPageNum = dataPage->rightSibPageNo;
          this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
        } else this->nextEntry++;
      }
    }
    if (traits::great(dataPage->keyArray[this->nextEntry],traits::getUpperBound(this))) {
      this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
      throw NoSuchKeyFoundException();
    }
    else if (this->highOp == LT && traits::equal(dataPage->keyArray[this->nextEntry], traits::getUpperBound(this))){
      this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
      throw NoSuchKeyFoundException();
    }
  } else {
    #ifdef DEBUG
    //assert(0);
    #endif
  }
}


template <typename keyType, class traits>
const void BTreeIndex::insertKeyTemplate(const void* key, const RecordId rid) {
  typedef typename traits::nonLeafType nonLeafType;
  typedef typename traits::leafType leafType;
  Page* rootPage;
  keyType keyValue = traits::getKeyValue(key);
  this->bufMgr->readPage(this->file, this->rootPageNum, rootPage);
  nonLeafType* rootData = reinterpret_cast<nonLeafType*>(rootPage);
  if (rootData->pageNoArray[0] == Page::INVALID_NUMBER) {
    Page* lessKey, *greaterKey;
    this->bufMgr->allocPage(this->file, rootData->pageNoArray[0], lessKey);
    this->bufMgr->allocPage(this->file, rootData->pageNoArray[1], greaterKey);

    leafType dataPageLeft, dataPageRight;
    memset(&dataPageLeft, 0, sizeof(leafType));
    memset(&dataPageRight, 0, sizeof(leafType));

    dataPageLeft.rightSibPageNo = rootData->pageNoArray[1];

    memcpy(lessKey, &dataPageLeft, sizeof(leafType));
    this->bufMgr->unPinPage(this->file, rootData->pageNoArray[0], true);

    traits::assign(dataPageRight.keyArray[0],keyValue);
    dataPageRight.ridArray[0] = rid;
    memcpy(greaterKey, &dataPageRight, sizeof(leafType));
    this->bufMgr->unPinPage(this->file, rootData->pageNoArray[1], true);

    rootData->level = 2;
    traits::assign(rootData->keyArray[0],keyValue);
    this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
  } else {
    PageId dataPageNum;
    PageId dataPageNumPrev;
    Page* tempPage;
    int insertAt = -1, endOfRecordsOffset;
    getPageNoAndOffsetOfKeyInsert<keyType>(key, rootPage, dataPageNum, insertAt, endOfRecordsOffset, dataPageNumPrev);
    this->bufMgr->readPage(this->file, dataPageNum, tempPage);
    leafType* dataPage = reinterpret_cast<leafType*>(tempPage);

    for (int j = endOfRecordsOffset; j > insertAt; j--) {
      dataPage->ridArray[j] = dataPage->ridArray[j-1];
      traits::assign(dataPage->keyArray[j],dataPage->keyArray[j-1]);
    }
    dataPage->ridArray[insertAt] = rid;
    traits::assign(dataPage->keyArray[insertAt],keyValue);
    this->bufMgr->unPinPage(this->file, dataPageNum, true);
#ifdef DEBUG
    std::cout << "DBG: Key " << keyValue << " inserted on page " << dataPageNum << " at offset " << insertAt << ":" << endOfRecordsOffset << std::endl;
#endif
#ifdef DEBUG
  if (this->attributeType != STRING) {
    bool rc = this->isStructureValid<keyType,traits>();
    assert(true == rc);
  }
#endif
  }
}

template<typename keyType, typename traits>
PageId BTreeIndex::getSiblingPage (Page* currPage, std::vector<std::pair<int,PageId>>& pathOfTraversal, bool dataPage, bool right, PageId parentPageId) {
  typedef typename traits::leafType leafType;
  typedef typename traits::nonLeafType nonLeafType;
  int size = pathOfTraversal.size();
  if (true == dataPage) {
    if (right == true) return reinterpret_cast<leafType*>(currPage)->rightSibPageNo;
    else {
#ifdef DEBUG
      assert(size > 0);
#endif
      PageId parentPageId = pathOfTraversal.back().second;
      int parentOffset = pathOfTraversal.back().first;
      Page* parentPage;
      if (parentOffset >= 1) {
        this->bufMgr->readPage(this->file, parentPageId, parentPage);
        nonLeafType* parentData = reinterpret_cast<nonLeafType*>(parentPage);
        int retval =  parentData->pageNoArray[parentOffset-1];
        this->bufMgr->unPinPage(this->file, parentPageId, true);
        return retval;
      } else if (parentPageId == this->rootPageNum) return Page::INVALID_NUMBER;
      else {
        int depth = 2;
        int offset;
        PageId retval = Page::INVALID_NUMBER;
        do {
          parentPageId = pathOfTraversal[size-depth].second;
          parentOffset = pathOfTraversal[size-depth].first;
          depth++;
          if (parentOffset >= 1) {
            this->bufMgr->readPage(this->file, parentPageId, parentPage);
            nonLeafType* parentData = reinterpret_cast<nonLeafType*>(parentPage);
            retval = parentData->pageNoArray[parentOffset-1];
            this->bufMgr->unPinPage(this->file, parentPageId, true);
            offset = size-depth+1;
            break;
          }
        } while ((size-depth >= 0) && pathOfTraversal[size-depth].second != this->rootPageNum);
        if (retval != Page::INVALID_NUMBER || pathOfTraversal[size-depth+1].first >= 1) {
          parentPageId = pathOfTraversal[offset].second;
          parentOffset = pathOfTraversal[offset].first;
          this->bufMgr->readPage(this->file, parentPageId, parentPage);
          nonLeafType* parentData = reinterpret_cast<nonLeafType*>(parentPage);
          this->bufMgr->unPinPage(this->file, parentPageId, true);
          parentPageId = parentData->pageNoArray[parentOffset-1];
          while (depth > 2) {
            this->bufMgr->readPage(this->file, parentPageId, parentPage);
            nonLeafType* parentData = reinterpret_cast<nonLeafType*>(parentPage);
            retval = parentData->pageNoArray[this->getOccupancy<keyType, traits>(parentPage, false)-1];
            this->bufMgr->unPinPage(this->file, parentPageId, true);
            parentPageId = retval;
            depth--;
          }
          return retval;
        } else return Page::INVALID_NUMBER;
      }
#ifdef DEBUG
      assert(0);
#endif
    }
  } else {
    if (right == false) {
      PageId parentPageId = pathOfTraversal[size-2].second;
      int parentOffset = pathOfTraversal[size-2].first;
      Page* parentPage;
      if (parentOffset >= 1) {
        this->bufMgr->readPage(this->file, parentPageId, parentPage);
        nonLeafType* parentData = reinterpret_cast<nonLeafType*>(parentPage);
        int retval =  parentData->pageNoArray[parentOffset-1];
        this->bufMgr->unPinPage(this->file, parentPageId, true);
        return retval;
      } else if (parentPageId == this->rootPageNum) return Page::INVALID_NUMBER;
      else {
        int depth = 2;
        int offset = -1;
        PageId retval = Page::INVALID_NUMBER;
        do {
          parentPageId = pathOfTraversal[size-depth].second;
          parentOffset = pathOfTraversal[size-depth].first;
          depth++;
          if (parentOffset >= 1) {
            this->bufMgr->readPage(this->file, parentPageId, parentPage);
            nonLeafType* parentData = reinterpret_cast<nonLeafType*>(parentPage);
            retval = parentData->pageNoArray[parentOffset-1];
            this->bufMgr->unPinPage(this->file, parentPageId, true);
            offset = size-depth+1;
            break;
          }
        } while ((size-depth >= 0) && pathOfTraversal[size-depth].second != this->rootPageNum);
        if (offset == -1) offset = 0;
        if (retval != Page::INVALID_NUMBER || pathOfTraversal[size-depth].first >= 1) {
          parentPageId = pathOfTraversal[offset].second;
          parentOffset = pathOfTraversal[offset].first;
          this->bufMgr->readPage(this->file, parentPageId, parentPage);
          nonLeafType* parentData = reinterpret_cast<nonLeafType*>(parentPage);
          this->bufMgr->unPinPage(this->file, parentPageId, true);
          parentPageId = parentData->pageNoArray[parentOffset-1];
          while (depth > 2) {
            this->bufMgr->readPage(this->file, parentPageId, parentPage);
            nonLeafType* parentData = reinterpret_cast<nonLeafType*>(parentPage);
            retval = parentData->pageNoArray[this->getOccupancy<keyType, traits>(parentPage, false)-1];
            this->bufMgr->unPinPage(this->file, parentPageId, true);
            parentPageId = retval;
            depth--;
          }
          return retval;
        } else return Page::INVALID_NUMBER;
      }
#ifdef DEBUG
      assert(0);
#endif
    } else {

#ifdef DEBUG
      assert(size > 0);
#endif
      PageId parentPageId = pathOfTraversal[size-2].second;
      int parentOffset = pathOfTraversal[size-2].first;
      Page* parentPage;
      if (parentOffset < traits::NONLEAFSIZE) {
        this->bufMgr->readPage(this->file, parentPageId, parentPage);
        nonLeafType* parentData = reinterpret_cast<nonLeafType*>(parentPage);
        int retval =  parentData->pageNoArray[parentOffset+1];
        this->bufMgr->unPinPage(this->file, parentPageId, true);
        return retval;
      } else if (parentPageId == this->rootPageNum) return Page::INVALID_NUMBER;
      else {
        int depth = 2;
        int offset=-1;
        PageId retval = Page::INVALID_NUMBER;
        do {
          parentPageId = pathOfTraversal[size-depth].second;
          parentOffset = pathOfTraversal[size-depth].first;
          depth++;
          if (parentOffset < traits::NONLEAFSIZE) {
            this->bufMgr->readPage(this->file, parentPageId, parentPage);
            nonLeafType* parentData = reinterpret_cast<nonLeafType*>(parentPage);
            retval = parentData->pageNoArray[parentOffset+1];
            this->bufMgr->unPinPage(this->file, parentPageId, true);
            offset = size-depth+1;
            break;
          }
        } while ((size-depth >= 0) && pathOfTraversal[size-depth].second != this->rootPageNum);
        if (offset == -1) offset = 0;
        if (retval != Page::INVALID_NUMBER || pathOfTraversal[size-depth].first < traits::NONLEAFSIZE) {
          parentPageId = pathOfTraversal[offset].second;
          parentOffset = pathOfTraversal[offset].first;
          this->bufMgr->readPage(this->file, parentPageId, parentPage);
          nonLeafType* parentData = reinterpret_cast<nonLeafType*>(parentPage);
          this->bufMgr->unPinPage(this->file, parentPageId, true);
          parentPageId = parentData->pageNoArray[parentOffset+1];
          while (depth > 2) {
            this->bufMgr->readPage(this->file, parentPageId, parentPage);
            nonLeafType* parentData = reinterpret_cast<nonLeafType*>(parentPage);
            retval = parentData->pageNoArray[0];
            this->bufMgr->unPinPage(this->file, parentPageId, true);
            parentPageId = retval;
            depth--;
          }
          return retval;
        } else return Page::INVALID_NUMBER;
      }
#ifdef DEBUG
      assert(0);
#endif
    }
  }
}

template<typename keyType, typename traits>
int BTreeIndex::getOccupancy(Page* currPage, bool dataPage) {
  typedef typename traits::nonLeafType nonLeafType;
  typedef typename traits::leafType leafType;
  int retval;
  if (dataPage == true) {
    leafType* dataPage = reinterpret_cast<leafType*>(currPage);
    retval = traits::LEAFSIZE;
    for (int i = 0; i < traits::LEAFSIZE; i++) {
      if (dataPage->ridArray[i].page_number == Page::INVALID_NUMBER) {
        retval = i;
        break;
      }
    }
#ifdef DEBUG
    assert(retval <= traits::LEAFSIZE);
#endif
  } else {
    nonLeafType* nonData = reinterpret_cast<nonLeafType*>(currPage);
    retval = traits::NONLEAFSIZE+1;
    for (int i = 0; i < traits::NONLEAFSIZE+1; i++) {
      if (nonData->pageNoArray[i] == Page::INVALID_NUMBER) {
        retval = i;
        break;
      }
    }
#ifdef DEBUG
    assert(retval <= traits::NONLEAFSIZE+1);
#endif
  }
  return retval;
}

template<typename keyType, typename traits>
void BTreeIndex::deleteEntryInLeaf(Page* leafPage, int startLoc, int endLoc) {
  typedef typename traits::leafType leafType;
#ifdef DEBUG
  assert(endLoc <= traits::LEAFSIZE);
  assert(startLoc >= 0);
  assert(startLoc <= endLoc);
#endif
  leafType* dataPage = reinterpret_cast<leafType*>(leafPage);
  for (int i = startLoc; i < endLoc-1; i++) {
    dataPage->ridArray[i] = dataPage->ridArray[i+1];
    traits::assign(dataPage->keyArray[i],dataPage->keyArray[i+1]);
  }
  dataPage->ridArray[endLoc-1].page_number = Page::INVALID_NUMBER;
  dataPage->ridArray[endLoc-1].slot_number = Page::INVALID_SLOT;
  traits::assign(dataPage->keyArray[endLoc-1], 0);
}

template<typename keyType, typename traits>
void BTreeIndex::deleteEntryInNonLeaf(Page* nonLeafPage, int startLoc, int endLoc) {
  typedef typename traits::nonLeafType nonLeafType;
#ifdef DEBUG
  assert(endLoc <= traits::NONLEAFSIZE+1);
  assert(startLoc >= 0);
  assert(startLoc <= endLoc);
#endif
  nonLeafType* parentPageData = reinterpret_cast<nonLeafType*>(nonLeafPage);
  int pageOffsetInParent = startLoc;
  int parentOccupancy = endLoc;
  if (pageOffsetInParent > 0) {
    for (int i = pageOffsetInParent; i < parentOccupancy-1; i++) {
      traits::assign(parentPageData->keyArray[i-1],parentPageData->keyArray[i]);
      if (i+1 <= traits::NONLEAFSIZE) parentPageData->pageNoArray[i] = parentPageData->pageNoArray[i+1];
    }
  } else if (pageOffsetInParent == 0) {
    for (int i = 0; i < parentOccupancy-1; i++) {
      if (i+1 <= parentOccupancy-2) traits::assign(parentPageData->keyArray[i],parentPageData->keyArray[i+1]);
      parentPageData->pageNoArray[i] = parentPageData->pageNoArray[i+1];
    }
  }
#ifdef DEBUG
  assert(parentOccupancy-1 >= 0);
#endif
  traits::assign(parentPageData->keyArray[parentOccupancy-2], 0);
  parentPageData->pageNoArray[parentOccupancy-1] = Page::INVALID_NUMBER;
}

template<typename keyType, typename traits>
void BTreeIndex::shiftRightPage (Page* leftSib, Page* page, int startLoc, int endLoc, int leftOccupancy, bool dataPage) {
  typedef typename traits::leafType leafType;
  typedef typename traits::nonLeafType nonLeafType;
  if (dataPage == true) {
#ifdef DEBUG
  assert(endLoc < traits::LEAFSIZE);
  assert(startLoc >= 0);
  assert(startLoc <= endLoc);
  assert(leftOccupancy < traits::LEAFSIZE);
#endif
    leafType* leftPageData = reinterpret_cast<leafType*>(leftSib);
    leafType* dataPage = reinterpret_cast<leafType*>(page);
    if (startLoc >= 0) {
#ifdef DEBUG
      assert(dataPage->ridArray[endLoc].page_number == Page::INVALID_NUMBER);
#endif
      for (int i = endLoc; i > 0; i--) {
        traits::assign(dataPage->keyArray[i], dataPage->keyArray[i-1]);
        dataPage->ridArray[i] = dataPage->ridArray[i-1];
      }
      this->deleteEntryInLeaf<keyType, traits>(reinterpret_cast<Page*>(dataPage), startLoc+1, endLoc+1);
    }
#ifdef DEBUG
      assert(leftOccupancy-1 >= 0);
      assert(leftPageData->ridArray[leftOccupancy-1].page_number != Page::INVALID_NUMBER);
#endif
    traits::assign(dataPage->keyArray[0], leftPageData->keyArray[leftOccupancy-1]);
    dataPage->ridArray[0] = leftPageData->ridArray[leftOccupancy-1];
    traits::assign(leftPageData->keyArray[leftOccupancy-1], 0);
    leftPageData->ridArray[leftOccupancy-1].page_number = Page::INVALID_NUMBER;
    leftPageData->ridArray[leftOccupancy-1].slot_number = Page::INVALID_SLOT;
  } else {
#ifdef DEBUG
  assert(endLoc < traits::NONLEAFSIZE+1);
  assert(startLoc >= 0);
  assert(startLoc <= endLoc);
  assert(leftOccupancy < traits::NONLEAFSIZE+1);
#endif
    nonLeafType* leftPageData = reinterpret_cast<nonLeafType*>(leftSib);
    nonLeafType* dataPage = reinterpret_cast<nonLeafType*>(page);
    if (startLoc >= 0) {
#ifdef DEBUG
      assert(dataPage->pageNoArray[endLoc] == Page::INVALID_NUMBER);
#endif
      for (int i = endLoc; i > 0; i--) {
        if (i-2 >= 0) traits::assign(dataPage->keyArray[i-1], dataPage->keyArray[i-2]);
        dataPage->pageNoArray[i] = dataPage->pageNoArray[i-1];
      }
      this->deleteEntryInNonLeaf<keyType, traits>(reinterpret_cast<Page*>(dataPage), startLoc+1, endLoc+1);
#ifdef DEBUG
      assert(leftOccupancy-1 >= 0);
      assert(leftPageData->pageNoArray[leftOccupancy-1] != Page::INVALID_NUMBER);
#endif
      dataPage->pageNoArray[0] = leftPageData->pageNoArray[leftOccupancy-1];
      leftPageData->pageNoArray[leftOccupancy-1] = Page::INVALID_NUMBER;
    }
  }
}

template<typename keyType, typename traits>
void BTreeIndex::shiftLeftPage (Page* rightSib, Page* page, int startLoc, int endLoc, int rightOccupancy, bool dataPage) {
  typedef typename traits::leafType leafType;
  typedef typename traits::nonLeafType nonLeafType;
  if (dataPage == true) {
#ifdef DEBUG
  assert(endLoc < traits::LEAFSIZE);
  assert(startLoc >= 0);
  assert(startLoc <= endLoc);
  assert(rightOccupancy <= traits::LEAFSIZE);
#endif
    leafType* rightPageData = reinterpret_cast<leafType*>(rightSib);
    leafType* dataPage = reinterpret_cast<leafType*>(page);
#ifdef DEBUG
    assert(dataPage->ridArray[endLoc].page_number == Page::INVALID_NUMBER);
    assert(rightPageData->ridArray[0].page_number != Page::INVALID_NUMBER);
#endif
    traits::assign(dataPage->keyArray[endLoc], rightPageData->keyArray[0]);
    dataPage->ridArray[endLoc] = rightPageData->ridArray[0];
    this->deleteEntryInLeaf<keyType, traits>(reinterpret_cast<Page*>(dataPage), startLoc, endLoc+1);

    for (int i = 0; i < rightOccupancy-1; i++) {
      traits::assign(rightPageData->keyArray[i], rightPageData->keyArray[i+1]);
      rightPageData->ridArray[i] = rightPageData->ridArray[i+1];
    }
#ifdef DEBUG
    assert(traits::equal(rightPageData->keyArray[rightOccupancy-1], rightPageData->keyArray[rightOccupancy-2]));
#endif
    traits::assign(rightPageData->keyArray[rightOccupancy-1], 0);
    rightPageData->ridArray[rightOccupancy-1].page_number = Page::INVALID_NUMBER;
    rightPageData->ridArray[rightOccupancy-1].slot_number = Page::INVALID_SLOT;
  } else {
#ifdef DEBUG
  assert(endLoc < traits::NONLEAFSIZE+1);
  assert(startLoc >= 0);
  assert(startLoc <= endLoc);
  assert(rightOccupancy < traits::NONLEAFSIZE+1);
#endif
    nonLeafType* rightPageData = reinterpret_cast<nonLeafType*>(rightSib);
    nonLeafType* dataPage = reinterpret_cast<nonLeafType*>(page);
    this->deleteEntryInNonLeaf<keyType, traits>(reinterpret_cast<Page*>(dataPage), startLoc, endLoc);
    
    dataPage->pageNoArray[endLoc-1] = rightPageData->pageNoArray[0];
    int i = 0;
    for (i = 0; i < rightOccupancy-2; i++) {
      traits::assign(rightPageData->keyArray[i], rightPageData->keyArray[i+1]);
      rightPageData->pageNoArray[i] = rightPageData->pageNoArray[i+1];
    }
    rightPageData->pageNoArray[i] = rightPageData->pageNoArray[i+1];
#ifdef DEBUG
    assert(rightOccupancy-2 >= 0);
    assert(traits::equal(rightPageData->keyArray[rightOccupancy-3], rightPageData->keyArray[rightOccupancy-2]));
    assert(rightPageData->pageNoArray[rightOccupancy-2] == rightPageData->pageNoArray[rightOccupancy-1]);
#endif
    traits::assign(rightPageData->keyArray[rightOccupancy-2], 0);
    rightPageData->pageNoArray[rightOccupancy-1] = Page::INVALID_NUMBER;
  }
}
template<typename keyType, typename traits>
void BTreeIndex::getLeftRightSiblingOccupancy (Page* currPage, PageId& leftSibling, Page*& leftSib, int& leftOccupancy, PageId& rightSibling, Page*& rightSib, int& rightOccupancy, std::vector<std::pair<int,PageId>>& pathOfTraversal, bool dataPage, PageId parentPageId) {
  leftSibling = this->getSiblingPage<keyType, traits>(currPage, pathOfTraversal, dataPage, false, parentPageId);
  leftOccupancy = -1;
  if (leftSibling != Page::INVALID_NUMBER) {
    this->bufMgr->readPage(this->file, leftSibling, leftSib);
    leftOccupancy = this->getOccupancy<keyType, traits>(leftSib, dataPage);
  }
  rightSibling = this->getSiblingPage<keyType, traits>(currPage, pathOfTraversal, dataPage, true, parentPageId);
  rightOccupancy = -1;
  if (rightSibling != Page::INVALID_NUMBER) {
    this->bufMgr->readPage(this->file, rightSibling, rightSib);
    rightOccupancy = this->getOccupancy<keyType, traits>(rightSib, dataPage);
  }
}

template<typename keyType, typename traits>
bool BTreeIndex::isStructureValid() {
  typedef typename traits::nonLeafType nonLeafType;
  typedef typename traits::leafType leafType;
  std::vector< typename std::pair< typename std::pair<PageId,keyType>, bool> > queue[2];
  Page* page;
  int depth = 1;
  nonLeafType* currData; 
  this->bufMgr->readPage(this->file, this->rootPageNum, page);
  currData = reinterpret_cast<nonLeafType*>(page);
  // for max = true
  std::pair<typename std::pair<PageId,keyType>, bool> minOrMaxConstraint;
  minOrMaxConstraint.first.first = this->rootPageNum;
  traits::assign(minOrMaxConstraint.first.second,currData->keyArray[0]);
  minOrMaxConstraint.second = true;
  queue[depth%2].push_back(minOrMaxConstraint);
  int maxDepth = currData->level;
  this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
  while (depth < maxDepth) {
    while (!queue[depth%2].empty())
    {
      this->bufMgr->readPage(this->file, queue[depth%2].back().first.first, page);
      currData = reinterpret_cast<nonLeafType*>(page);
      minOrMaxConstraint.first.first = currData->pageNoArray[0];
      traits::assign(minOrMaxConstraint.first.second,currData->keyArray[0]);
      minOrMaxConstraint.second = true;
      queue[(depth+1)%2].push_back(minOrMaxConstraint);
      for (int i = 0; i < traits::NONLEAFSIZE; i++) {
        if (!(i+2 > traits::NONLEAFSIZE || currData->pageNoArray[i+2] == Page::INVALID_NUMBER)) {
#ifdef DEBUG
          assert(currData->level <= maxDepth);
          assert(traits::less(currData->keyArray[i], currData->keyArray[i+1]));
#endif
          minOrMaxConstraint.first.first = currData->pageNoArray[i+1];
          traits::assign(minOrMaxConstraint.first.second, currData->keyArray[i]);
          minOrMaxConstraint.second = false;
          queue[(depth+1)%2].push_back(minOrMaxConstraint);
          if (this->rootPageNum != queue[depth%2].back().first.first) {
            minOrMaxConstraint = queue[depth%2].back();
            bool checkMax = minOrMaxConstraint.second;
#ifdef DEBUG
            if (true == checkMax) {
              assert(traits::lessE(currData->keyArray[i],minOrMaxConstraint.first.second));
            } else {
              assert(traits::greatE(currData->keyArray[i],minOrMaxConstraint.first.second));
            }
#endif
          }
        } else { 
          minOrMaxConstraint.first.first = currData->pageNoArray[i+1];
          traits::assign(minOrMaxConstraint.first.second, currData->keyArray[i]);
          minOrMaxConstraint.second = false;
          queue[(depth+1)%2].push_back(minOrMaxConstraint);
#ifdef DEBUG
          assert(i+1 >= traits::NONLEAFSIZE || traits::equal(currData->keyArray[i+1],0));
#endif
          if (this->rootPageNum != queue[depth%2].back().first.first) {
            minOrMaxConstraint = queue[depth%2].back();
            bool checkMax = minOrMaxConstraint.second;
#ifdef DEBUG
            if (true == checkMax) {
              assert(traits::lessE(currData->keyArray[i],minOrMaxConstraint.first.second));
            } else {
              assert(traits::greatE(currData->keyArray[i],minOrMaxConstraint.first.second));
            }
#endif
          }
          break;
        }
      }
      this->bufMgr->unPinPage(this->file, queue[depth%2].back().first.first, true);
      queue[depth%2].pop_back();
    }
    depth++;
  }
  while (!queue[depth%2].empty()) {
    this->bufMgr->readPage(this->file, queue[depth%2].back().first.first, page);
    leafType* data = reinterpret_cast<leafType*>(page);
    for (int i = 1; i < traits::LEAFSIZE; i++) {
      if (data->ridArray[i].page_number == Page::INVALID_NUMBER) break;
#ifdef DEBUG 
      assert(traits::greatE(data->keyArray[i],data->keyArray[i-1]));
#endif
      minOrMaxConstraint = queue[depth%2].back();
      bool checkMax = minOrMaxConstraint.second;
#ifdef DEBUG 
      if (true == checkMax) {
        assert(traits::lessE(data->keyArray[i-1],minOrMaxConstraint.first.second));
      } else {
        assert(traits::greatE(data->keyArray[i-1],minOrMaxConstraint.first.second));
      }
#endif
    }
    this->bufMgr->unPinPage(this->file, queue[depth%2].back().first.first, true);
    queue[depth%2].pop_back();
  }
  return true;
}

template<typename keyType, typename traits>
const bool BTreeIndex::deleteKeyTemplate(const void* key) {
  typedef typename traits::nonLeafType nonLeafType;
  typedef typename traits::leafType leafType;
  Page* rootPage;
  keyType keyValue = traits::getKeyValue(key);
#ifdef DEBUG
    std::cout << "DBG: Key " << keyValue << " deleted through ";
#endif
  this->bufMgr->readPage(this->file, this->rootPageNum, rootPage);
  nonLeafType* rootData = reinterpret_cast<nonLeafType*>(rootPage);
  nonLeafType* currPage = rootData;
  std::vector<std::pair<int,PageId>> pathOfTraversal;
  bool retval = true;
  PageId lastPage = this->rootPageNum;
  Page* tempPage;
  int depth = 1, i = 0;
  while (depth < rootData->level) {
    if (traits::less(keyValue,currPage->keyArray[0])) {
      // Case smaller than all keys
      i = 0;
      pathOfTraversal.push_back(std::pair<int,PageId>(i, lastPage));
    } else {
      // invariant page[i] contains keys >= key[i-1]
      // invariant page[i] contains keys < key [i]
      for (i = 0; i < traits::NONLEAFSIZE; i++) {
        if (currPage->pageNoArray[i+1] == Page::INVALID_NUMBER) {
          pathOfTraversal.push_back(std::pair<int,PageId>(i, lastPage));
          break;
        }
        /* 1st page contains keys greater than key[0] so if keyValue is greater than key[1]:
         * the key must lie in page[2] or ahead. Since page[2] contains keys greater than key[1] */
        if (traits::lessE(currPage->keyArray[i],keyValue)) {
          // If the next page is not invalid, it might contain the key, so continue.
#ifdef DEBUG
          // keys all smaller than keyArray[i] should lie in this page so it should be valid
          assert(currPage->pageNoArray[i] != Page::INVALID_NUMBER);
          // keys all greater than equal to keyArray[i] should lie in page[i+1] or ahead.
          assert(currPage->pageNoArray[i+1] != Page::INVALID_NUMBER);
#endif
          continue; // means if this was the last page in the node, we need to add to this page only otherwise continue
        }
        pathOfTraversal.push_back(std::pair<int,PageId>(i, lastPage));
        break;
      }
    }
    if (i == traits::NONLEAFSIZE) {
      pathOfTraversal.push_back(std::pair<int,PageId>(i, lastPage));
    }
    this->bufMgr->unPinPage(this->file, lastPage, false);
    this->bufMgr->readPage(this->file, currPage->pageNoArray[i], tempPage);
    lastPage = currPage->pageNoArray[i];
    currPage = reinterpret_cast<nonLeafType*>(tempPage);
    depth++;
  }
  PageId dataPageId = lastPage;
  i = 0;
  int startLoc = traits::LEAFSIZE, endLoc;
  leafType* dataPage = reinterpret_cast<leafType*>(currPage);
  for (i = 0; i < traits::LEAFSIZE; i++) {
    if (dataPage->ridArray[i].page_number == Page::INVALID_NUMBER) {
      if (startLoc == traits::LEAFSIZE) startLoc = i;
      break;
    }
    if (traits::great(keyValue,dataPage->keyArray[i])) continue;
    if (startLoc == traits::LEAFSIZE) {
      startLoc = i;
    }
  }
#ifdef DEBUG
  assert(pathOfTraversal.size());
  assert(startLoc < traits::LEAFSIZE);
#endif
  PageId parentPageId = pathOfTraversal.back().second;
  int parentPageOffset = pathOfTraversal.back().first;
  endLoc = i <= traits::LEAFSIZE ? i : traits::LEAFSIZE;
  if (!traits::equal(dataPage->keyArray[startLoc],keyValue)) retval = false;
  if (true == retval) {
    if (endLoc > traits::LEAFSIZE/2) {
      this->deleteEntryInLeaf<keyType, traits>(reinterpret_cast<Page*>(dataPage), startLoc, endLoc);
      this->bufMgr->unPinPage(this->file, dataPageId, true);
#ifdef DEBUG 
      std::cout << "normal operation." << std::endl;
#endif
    } else {
      this->bufMgr->readPage(this->file, this->rootPageNum, rootPage);
      rootData = reinterpret_cast<nonLeafType*>(rootPage);
      int rootOccupancy = this->getOccupancy<keyType, traits>(rootPage, false);
      if (rootData->level > 2 || rootOccupancy > 2) {
        this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
        PageId rightSibling, leftSibling; 
        Page* rightSib, *leftSib;
        int rightOccupancy = -1, leftOccupancy = -1;
        this->getLeftRightSiblingOccupancy<keyType, traits> (reinterpret_cast<Page*>(dataPage), leftSibling, leftSib, leftOccupancy,
            rightSibling, rightSib, rightOccupancy, pathOfTraversal, true, Page::INVALID_NUMBER);
        if (rightOccupancy > traits::LEAFSIZE/2) {
          // rotate leftwards from right page
          this->shiftLeftPage<keyType, traits>(rightSib, reinterpret_cast<Page*>(dataPage), startLoc, endLoc, rightOccupancy, true);

          Page* parentPage;
          this->bufMgr->readPage(this->file, parentPageId, parentPage);
          nonLeafType* parentPageData = reinterpret_cast<nonLeafType*>(parentPage);
          int parentOccupancy = this->getOccupancy<keyType, traits>(parentPage, false);
#ifdef DEBUG
          assert(parentPageOffset >= 0);
#endif
          if (parentPageOffset < parentOccupancy-1) traits::assign(parentPageData->keyArray[parentPageOffset], reinterpret_cast<leafType*>(rightSib)->keyArray[0]);
          else {
            int size = pathOfTraversal.size();
            int k = 2;
            while (size - k >= 0) {
              PageId parentParentPageId = pathOfTraversal[size-k].second;
              int parentParentPageOffset = pathOfTraversal[size-k].first;
              Page* tempPage;
              this->bufMgr->readPage(this->file, parentParentPageId, tempPage);
              nonLeafType* tempData = reinterpret_cast<nonLeafType*>(tempPage);
              int tempOccupancy = this->getOccupancy<keyType, traits>(tempPage, false);
              if (parentParentPageOffset < tempOccupancy -1) {
                traits::assign(tempData->keyArray[parentParentPageOffset], reinterpret_cast<leafType*>(rightSib)->keyArray[0]);
                this->bufMgr->unPinPage(this->file, parentParentPageId, true);
                break;
              }
              else {
                k++;
                this->bufMgr->unPinPage(this->file, parentParentPageId, true);
                continue;
              }
            }
          }

          this->bufMgr->unPinPage(this->file, dataPageId, true);
          if (-1 != leftOccupancy) this->bufMgr->unPinPage(this->file, leftSibling, false);
          if (-1 != rightOccupancy) this->bufMgr->unPinPage(this->file, rightSibling, true);
          this->bufMgr->unPinPage(this->file, parentPageId, true);
#ifdef DEBUG 
          std::cout << "rotating leftwards from right page." << std::endl;
#endif
        } else if (leftOccupancy > traits::LEAFSIZE/2) {
          // rotate rightwards from left page
          this->shiftRightPage<keyType, traits>(leftSib, reinterpret_cast<Page*>(dataPage), startLoc, endLoc, leftOccupancy, true);

          Page* parentPage;
          this->bufMgr->readPage(this->file, parentPageId, parentPage);
          nonLeafType* parentPageData = reinterpret_cast<nonLeafType*>(parentPage);
#ifdef DEBUG
          assert(parentPageOffset-1 < traits::NONLEAFSIZE);
#endif
          if (parentPageOffset >= 1) traits::assign(parentPageData->keyArray[parentPageOffset-1], dataPage->keyArray[0]);
          else {
            int size = pathOfTraversal.size();
            int k = 2;
            while (size - k >= 0) {
              PageId parentParentPageId = pathOfTraversal[size-k].second;
              int parentParentPageOffset = pathOfTraversal[size-k].first;
              Page* tempPage;
              this->bufMgr->readPage(this->file, parentParentPageId, tempPage);
              nonLeafType* tempData = reinterpret_cast<nonLeafType*>(tempPage);
              if (parentParentPageOffset >= 1) {
                traits::assign(tempData->keyArray[parentParentPageOffset-1], dataPage->keyArray[0]);
                this->bufMgr->unPinPage(this->file, parentParentPageId, true);
                break;
              }
              else {
                k++;
                this->bufMgr->unPinPage(this->file, parentParentPageId, true);
                continue;
              }
            }
          }

          this->bufMgr->unPinPage(this->file, dataPageId, true);
          if(-1 != leftOccupancy) this->bufMgr->unPinPage(this->file, leftSibling, true);
          if (-1 != rightOccupancy) this->bufMgr->unPinPage(this->file, rightSibling, false);
          this->bufMgr->unPinPage(this->file, parentPageId, true);
#ifdef DEBUG 
          std::cout << "rotating rightwards from left page." << std::endl;
#endif
        } else if (leftOccupancy != -1) {
#ifdef DEBUG 
          std::cout << "merging with left page." << std::endl;
#endif
          // merge with left page default, as copying in upper half array is easier
          this->deleteEntryInLeaf<keyType, traits>(reinterpret_cast<Page*>(dataPage), startLoc, endLoc);
          leafType* leftPageData = reinterpret_cast<leafType*>(leftSib);
          leftPageData->rightSibPageNo = rightSibling;
#ifdef DEBUG
          assert(leftPageData->ridArray[leftOccupancy].page_number == Page::INVALID_NUMBER);
#endif
          for (int i = leftOccupancy,j=0 ; i < traits::LEAFSIZE; i++, j++) {
            traits::assign(leftPageData->keyArray[i],dataPage->keyArray[j]);
            leftPageData->ridArray[i] = dataPage->ridArray[j];
          }
          this->bufMgr->unPinPage(this->file, dataPageId, true);
          bool rotated = false;
          while (pathOfTraversal.size() >= 1) {
            parentPageOffset = pathOfTraversal.back().first;
            parentPageId = pathOfTraversal.back().second;
            Page* parentPage;
            this->bufMgr->readPage(this->file, parentPageId, parentPage);
            int parentOccupancy = this->getOccupancy<keyType, traits>(parentPage, false);
            nonLeafType* parentPageData = reinterpret_cast<nonLeafType*>(parentPage);
            if (parentPageId == this->rootPageNum || parentOccupancy > traits::NONLEAFSIZE/2) {
              if (parentOccupancy > 2) {
                if (parentPageOffset > 0) this->deleteEntryInNonLeaf<keyType, traits> (parentPage, parentPageOffset, parentOccupancy);
                else {
                  keyType copyUpKey = keyType(0);
                  traits::assign(copyUpKey, parentPageData->keyArray[0]);
                  this->deleteEntryInNonLeaf<keyType, traits> (parentPage, parentPageOffset, parentOccupancy);
                  int size = pathOfTraversal.size();
                  int k = 2;
                  while (size - k >= 0) {
                    PageId parentParentPageId = pathOfTraversal[size-k].second;
                    int parentParentPageOffset = pathOfTraversal[size-k].first;
                    Page* tempPage;
                    this->bufMgr->readPage(this->file, parentParentPageId, tempPage);
                    nonLeafType* tempData = reinterpret_cast<nonLeafType*>(tempPage);
                    if (parentParentPageOffset >= 1) {
                      traits::assign(tempData->keyArray[parentParentPageOffset-1], copyUpKey);
                      this->bufMgr->unPinPage(this->file, parentParentPageId, true);
                      break;
                    }
                    else {
                      k++;
                      this->bufMgr->unPinPage(this->file, parentParentPageId, true);
                      continue;
                    }
                  }
                }
              } else if (parentOccupancy == 2) {
#ifdef DEBUG
                assert(parentPageId == this->rootPageNum);
#endif
                traits::assign(parentPageData->keyArray[0], keyValue);
                this->rootPageNum = parentPageData->pageNoArray[0];
              } else {
#ifdef DEBUG
                assert(0);
#endif
              }
              this->bufMgr->unPinPage(this->file, leftSibling, true);
              if (rightOccupancy != -1) this->bufMgr->unPinPage(this->file, rightSibling, false);
              this->bufMgr->unPinPage(this->file, parentPageId, true);
              break;
            } else {
              PageId pRightSibling, pLeftSibling; 
              Page* pRightSib, *pLeftSib;
              int pRightOccupancy = -1, pLeftOccupancy = -1;
              this->getLeftRightSiblingOccupancy<keyType, traits> (parentPage, pLeftSibling, pLeftSib, pLeftOccupancy,
                  pRightSibling, pRightSib, pRightOccupancy, pathOfTraversal, false, parentPageId);
              if (pRightOccupancy > traits::NONLEAFSIZE/2) {
                rotated = true;
                pathOfTraversal.pop_back();
                if (!pathOfTraversal.empty()) {
                  PageId parentParentPageOffset = pathOfTraversal.back().first;
                  int parentParentPageId = pathOfTraversal.back().second;
                  Page* parentParentPage;
                  this->bufMgr->readPage(this->file, parentParentPageId, parentParentPage);
                  nonLeafType* parentParentPageData = reinterpret_cast<nonLeafType*>(parentParentPage);
                  nonLeafType* rightPageData = reinterpret_cast<nonLeafType*>(pRightSib);
                  keyType rightPageFirstKey = keyType(0);
                  traits::assign(rightPageFirstKey, rightPageData->keyArray[0]);
                  if (parentPageOffset > 0) this->shiftLeftPage<keyType, traits>(pRightSib, parentPage, parentPageOffset, parentOccupancy, pRightOccupancy, false);
                  else {
                    keyType copyUpKey = keyType(0);
                    traits::assign(copyUpKey, parentPageData->keyArray[0]);
                    this->shiftLeftPage<keyType, traits>(pRightSib, parentPage, parentPageOffset, parentOccupancy, pRightOccupancy, false);
                    int size = pathOfTraversal.size();
                    int k = 1;
                    while (size - k >= 0) {
                      PageId parentParentPageId = pathOfTraversal[size-k].second;
                      int parentParentPageOffset = pathOfTraversal[size-k].first;
                      Page* tempPage;
                      this->bufMgr->readPage(this->file, parentParentPageId, tempPage);
                      nonLeafType* tempData = reinterpret_cast<nonLeafType*>(tempPage);
                      if (parentParentPageOffset >= 1) {
                        traits::assign(tempData->keyArray[parentParentPageOffset-1], copyUpKey);
                        this->bufMgr->unPinPage(this->file, parentParentPageId, true);
                        break;
                      }
                      else {
                        k++;
                        this->bufMgr->unPinPage(this->file, parentParentPageId, true);
                        continue;
                      }
                    }
                  }
                  traits::assign(parentPageData->keyArray[parentOccupancy-2], parentParentPageData->keyArray[parentParentPageOffset]);
                  traits::assign(parentParentPageData->keyArray[parentParentPageOffset], rightPageFirstKey);
                  this->bufMgr->unPinPage(this->file, parentParentPageId, true);
#ifdef DEBUG 
                  std::cout << "Parent non-leaf node rotated left." << std::endl;
#endif
                } else {
#ifdef DEBUG 
                  assert(0);
#endif
                }
              } else if (pLeftOccupancy > traits::NONLEAFSIZE/2) {
                rotated = true;
                if(parentPageOffset>0) this->shiftRightPage<keyType, traits>(pLeftSib, parentPage, parentPageOffset, parentOccupancy, pLeftOccupancy, false);
                else {
                  keyType copyUpKey = keyType(0);
                  traits::assign(copyUpKey, parentPageData->keyArray[0]);
                  this->shiftRightPage<keyType, traits>(pLeftSib, parentPage, parentPageOffset, parentOccupancy, pLeftOccupancy, false);
                  int size = pathOfTraversal.size();
                  int k = 1;
                  while (size - k >= 0) {
                    PageId parentParentPageId = pathOfTraversal[size-k].second;
                    int parentParentPageOffset = pathOfTraversal[size-k].first;
                    Page* tempPage;
                    this->bufMgr->readPage(this->file, parentParentPageId, tempPage);
                    nonLeafType* tempData = reinterpret_cast<nonLeafType*>(tempPage);
                    if (parentParentPageOffset >= 1) {
                      traits::assign(tempData->keyArray[parentParentPageOffset-1], copyUpKey);
                      this->bufMgr->unPinPage(this->file, parentParentPageId, true);
                      break;
                    }
                    else {
                      k++;
                      this->bufMgr->unPinPage(this->file, parentParentPageId, true);
                      continue;
                    }
                  }
                }
                pathOfTraversal.pop_back();
                if (!pathOfTraversal.empty()) {
                  PageId parentParentPageOffset = pathOfTraversal.back().first;
                  int parentParentPageId = pathOfTraversal.back().second;
                  Page* parentParentPage;
                  this->bufMgr->readPage(this->file, parentParentPageId, parentParentPage);
                  nonLeafType* parentParentPageData = reinterpret_cast<nonLeafType*>(parentParentPage);
                  nonLeafType* leftPageData = reinterpret_cast<nonLeafType*>(pLeftSib);
                  if (parentParentPageOffset >= 1) {
                    traits::assign(parentPageData->keyArray[0], parentParentPageData->keyArray[parentParentPageOffset-1]);
                    traits::assign(parentParentPageData->keyArray[parentParentPageOffset-1], leftPageData->keyArray[pLeftOccupancy-2]);
                    traits::assign(leftPageData->keyArray[pLeftOccupancy-2], 0);
                  } else {
                    int size = pathOfTraversal.size();
                    int k = 1;
                    while (size - k >= 0) {
                      PageId parentParentPageId = pathOfTraversal[size-k].second;
                      int parentParentPageOffset = pathOfTraversal[size-k].first;
                      Page* tempPage;
                      this->bufMgr->readPage(this->file, parentParentPageId, tempPage);
                      nonLeafType* tempData = reinterpret_cast<nonLeafType*>(tempPage);
                      if (parentParentPageOffset >= 1) {
                        traits::assign(parentPageData->keyArray[0], tempData->keyArray[parentParentPageOffset-1]);
                        traits::assign(tempData->keyArray[parentParentPageOffset-1], leftPageData->keyArray[pLeftOccupancy-2]);
                        this->bufMgr->unPinPage(this->file, parentParentPageId, true);
                        break;
                      }
                      else {
                        k++;
                        this->bufMgr->unPinPage(this->file, parentParentPageId, true);
                        continue;
                      }
                    }
                  }
                  this->bufMgr->unPinPage(this->file, parentParentPageId, true);
#ifdef DEBUG 
                  std::cout << "Parent non-leaf node rotated right." << std::endl;
#endif
                } else {
#ifdef DEBUG 
                  assert(0);
#endif
                }
              } else if (pLeftOccupancy != -1) {
                this->deleteEntryInNonLeaf<keyType, traits>(parentPage, parentPageOffset, parentOccupancy);
                nonLeafType* leftPageData = reinterpret_cast<nonLeafType*>(pLeftSib);
                for (int i = pLeftOccupancy,j=1 ; i < traits::NONLEAFSIZE; i++, j++) {
                  traits::assign(leftPageData->keyArray[i],parentPageData->keyArray[j-1]);
                  leftPageData->pageNoArray[i] = parentPageData->pageNoArray[j-1];
                }
                pathOfTraversal.pop_back();
                if (!pathOfTraversal.empty()) {
                  PageId parentParentPageOffset = pathOfTraversal.back().first;
                  int parentParentPageId = pathOfTraversal.back().second;
                  Page* parentParentPage;
                  this->bufMgr->readPage(this->file, parentParentPageId, parentParentPage);
                  nonLeafType* parentParentPageData = reinterpret_cast<nonLeafType*>(parentParentPage);
#ifdef DEBUG 
                  assert(parentParentPageOffset >= 1);
#endif
                  traits::assign(leftPageData->keyArray[pLeftOccupancy-1], parentParentPageData->keyArray[parentParentPageOffset-1]);
                  this->bufMgr->unPinPage(this->file, parentParentPageId, true);
#ifdef DEBUG 
                  std::cout << "Parent non-leaf node merged with left." << std::endl;
#endif
                } else {
#ifdef DEBUG 
                  assert(0);
#endif
                }
              } else if (pRightOccupancy != -1) {
                this->deleteEntryInNonLeaf<keyType, traits>(parentPage, parentPageOffset, parentOccupancy);
                parentOccupancy-= 1;
                nonLeafType* rightPageData = reinterpret_cast<nonLeafType*>(pRightSib);
                for (int i = parentOccupancy,j = 0; i < traits::NONLEAFSIZE; j++,i++) {
                  traits::assign(parentPageData->keyArray[i],rightPageData->keyArray[j]);
                  parentPageData->pageNoArray[i] = rightPageData->pageNoArray[j];
                }
                pathOfTraversal.pop_back();
                if (!pathOfTraversal.empty()) {
                  PageId parentParentPageOffset = pathOfTraversal.back().first;
                  int parentParentPageId = pathOfTraversal.back().second;
                  Page* parentParentPage;
                  this->bufMgr->readPage(this->file, parentParentPageId, parentParentPage);
                  nonLeafType* parentParentPageData = reinterpret_cast<nonLeafType*>(parentParentPage);
                  int size = pathOfTraversal.size();
                  if (parentParentPageOffset == 0) {
                    this->bufMgr->unPinPage(this->file, parentParentPageId, true);
                    int k = 1;
                    while (size - k >= 0) {
                      PageId tempParentPageId = pathOfTraversal[size-k].second;
                      int tempParentPageOffset = pathOfTraversal[size-k].first;
                      Page* tempPage;
                      this->bufMgr->readPage(this->file, tempParentPageId, tempPage);
                      if (tempParentPageOffset >= 1) {
                        parentParentPageData = reinterpret_cast<nonLeafType*>(tempPage);
                        parentParentPageOffset = tempParentPageOffset-1;
                        parentParentPageId = tempParentPageId;
                        traits::assign(parentPageData->keyArray[parentOccupancy-1], parentParentPageData->keyArray[parentParentPageOffset]);
                        this->bufMgr->unPinPage(this->file, tempParentPageId, true);
                        break;
                      }
                      else {
                        k++;
                        if (size -k < 0) {
                          parentParentPageOffset = 0;
                          traits::assign(parentPageData->keyArray[parentOccupancy-1], parentParentPageData->keyArray[parentParentPageOffset]);
                          this->bufMgr->unPinPage(this->file, parentParentPageId, true);
                          break;
                        }
                        this->bufMgr->unPinPage(this->file, tempParentPageId, true);
                      }
                    }
                  } else {
                    parentParentPageOffset = parentParentPageOffset-1;
                    traits::assign(parentPageData->keyArray[parentOccupancy-1], parentParentPageData->keyArray[parentParentPageOffset]);
                    this->bufMgr->unPinPage(this->file, parentParentPageId, true);
                  }
                  if (pathOfTraversal.back().first == 0) pathOfTraversal[size-1].first = 1;
#ifdef DEBUG 
                  std::cout << "Parent non-leaf node merged with right." << std::endl;
#endif
                } else {
#ifdef DEBUG 
                  assert(0);
#endif
                }
              } else {
#ifdef DEBUG 
                std::cout << "Non-leaf page made root." << std::endl;
#endif
                this->deleteEntryInNonLeaf<keyType, traits>(parentPage, parentPageOffset, parentOccupancy);
                this->rootPageNum = parentPageId;
                if (leftOccupancy != -1) this->bufMgr->unPinPage(this->file, leftSibling, true);
                if (rightOccupancy != -1) this->bufMgr->unPinPage(this->file, rightSibling, true);
                this->bufMgr->unPinPage(this->file, parentPageId, true);
                break;
              }
              if(-1 != pLeftOccupancy) this->bufMgr->unPinPage(this->file, pLeftSibling, true);
              if(-1 != pRightOccupancy) this->bufMgr->unPinPage(this->file, pRightSibling, true);
              this->bufMgr->unPinPage(this->file, parentPageId, true);
            }
            if (true == rotated) {
              if(-1 != leftOccupancy) this->bufMgr->unPinPage(this->file, leftSibling, true);
              if(-1 != rightOccupancy) this->bufMgr->unPinPage(this->file, rightSibling, true);
              break;
            }
          }
        } else if (rightOccupancy != -1) {
          this->deleteEntryInLeaf<keyType, traits>(reinterpret_cast<Page*>(dataPage), startLoc, endLoc);
          endLoc -= 1;
          leafType* rightPageData = reinterpret_cast<leafType*>(rightSib);
          dataPage->rightSibPageNo = rightPageData->rightSibPageNo;
          for (int i = endLoc,j = 0; i < traits::LEAFSIZE; j++,i++) {
            traits::assign(dataPage->keyArray[i],rightPageData->keyArray[j]);
            dataPage->ridArray[i] = rightPageData->ridArray[j];
          }
          this->bufMgr->unPinPage(this->file, dataPageId, true);
          bool rotated = false;
          while (pathOfTraversal.size() >= 1) {
            parentPageOffset = pathOfTraversal.back().first;
            parentPageId = pathOfTraversal.back().second;
            Page* parentPage;
            this->bufMgr->readPage(this->file, parentPageId, parentPage);
            int parentOccupancy = this->getOccupancy<keyType, traits>(parentPage, false);
            nonLeafType* parentPageData = reinterpret_cast<nonLeafType*>(parentPage);
            if (parentPageId == this->rootPageNum || parentOccupancy > traits::NONLEAFSIZE/2) {
              if (parentPageOffset+1 <= traits::NONLEAFSIZE){
                if (parentPageOffset == 0 && this->rootPageNum == parentPageId && parentOccupancy == 2) {
                  traits::assign(parentPageData->keyArray[0], rightPageData->keyArray[rightOccupancy-1]);
                  parentPageData->pageNoArray[1] = Page::INVALID_NUMBER;
                  this->rootPageNum = parentPageData->pageNoArray[0];
                } else {
                  this->deleteEntryInNonLeaf<keyType, traits> (parentPage, parentPageOffset+1, parentOccupancy);
                }
              } else {
#ifdef DEBUG 
                assert(0);
#endif
              }
              if (leftOccupancy != -1) this->bufMgr->unPinPage(this->file, leftSibling, false);
              this->bufMgr->unPinPage(this->file, rightSibling, true);
              this->bufMgr->unPinPage(this->file, parentPageId, true);
              break;
            } else {
              PageId pRightSibling, pLeftSibling;
              Page* pRightSib, *pLeftSib;
              int pRightOccupancy = -1, pLeftOccupancy = -1;
              this->getLeftRightSiblingOccupancy<keyType, traits> (parentPage, pLeftSibling, pLeftSib, pLeftOccupancy,
                  pRightSibling, pRightSib, pRightOccupancy, pathOfTraversal, false, parentPageId);
              if (pRightOccupancy > traits::NONLEAFSIZE/2) {
                rotated = true;
                pathOfTraversal.pop_back();
                if (!pathOfTraversal.empty()) {
                  PageId parentParentPageOffset = pathOfTraversal.back().first;
                  int parentParentPageId = pathOfTraversal.back().second;
                  Page* parentParentPage;
                  this->bufMgr->readPage(this->file, parentParentPageId, parentParentPage);
                  nonLeafType* parentParentPageData = reinterpret_cast<nonLeafType*>(parentParentPage);
                  nonLeafType* rightPageData = reinterpret_cast<nonLeafType*>(pRightSib);
                  keyType rightPageFirstKey = keyType(0);
                  traits::assign(rightPageFirstKey, rightPageData->keyArray[0]);
                  if (parentPageOffset+1 <= traits::NONLEAFSIZE) this->shiftLeftPage<keyType, traits>(pRightSib, parentPage, parentPageOffset+1, parentOccupancy, pRightOccupancy, false);
                  else {
#ifdef DEBUG 
                    assert(0);
#endif
                  }
                  if (parentParentPageOffset == 0) {
                    this->bufMgr->unPinPage(this->file, parentParentPageId, true);
                    int k = 1;
                    int size = pathOfTraversal.size();
                    while (size - k >= 0) {
                      PageId tempParentPageId = pathOfTraversal[size-k].second;
                      int tempParentPageOffset = pathOfTraversal[size-k].first;
                      Page* tempPage;
                      this->bufMgr->readPage(this->file, tempParentPageId, tempPage);
                      if (tempParentPageOffset >= 1) {
                        parentParentPageData = reinterpret_cast<nonLeafType*>(tempPage);
                        parentParentPageOffset = tempParentPageOffset-1;
                        parentParentPageId = tempParentPageId;
                        traits::assign(parentPageData->keyArray[parentOccupancy-2], parentParentPageData->keyArray[parentParentPageOffset]);
                        traits::assign(parentParentPageData->keyArray[parentParentPageOffset], rightPageFirstKey);
                        this->bufMgr->unPinPage(this->file, tempParentPageId, true);
                        break;
                      }
                      else {
                        k++;
                        if (size -k < 0) {
                          parentParentPageOffset = 0;
                          traits::assign(parentPageData->keyArray[parentOccupancy-2], parentParentPageData->keyArray[parentParentPageOffset]);
                          traits::assign(parentParentPageData->keyArray[parentParentPageOffset], rightPageFirstKey);
                          this->bufMgr->unPinPage(this->file, parentParentPageId, true);
                          break;
                        }
                        this->bufMgr->unPinPage(this->file, tempParentPageId, true);
                      }
                    }
                  } else {
                    parentParentPageOffset = parentParentPageOffset-1;
                    traits::assign(parentPageData->keyArray[parentOccupancy-2], parentParentPageData->keyArray[parentParentPageOffset]);
                    traits::assign(parentParentPageData->keyArray[parentParentPageOffset], rightPageFirstKey);
                    this->bufMgr->unPinPage(this->file, parentParentPageId, true);
                  }
#ifdef DEBUG 
                  std::cout << "Parent non-leaf node rotated left." << std::endl;
#endif
                } else {
#ifdef DEBUG 
                  assert(0);
#endif
                }
              } else if (pLeftOccupancy > traits::NONLEAFSIZE/2) {
                rotated = true;
                if (parentPageOffset+1 <= traits::NONLEAFSIZE) this->shiftRightPage<keyType, traits>(pLeftSib, parentPage, parentPageOffset+1, parentOccupancy, pLeftOccupancy, false);
                else {
#ifdef DEBUG 
                  assert(0);
#endif
                }
                pathOfTraversal.pop_back();
                if (!pathOfTraversal.empty()) {
                  PageId parentParentPageOffset = pathOfTraversal.back().first;
                  int parentParentPageId = pathOfTraversal.back().second;
                  Page* parentParentPage;
                  this->bufMgr->readPage(this->file, parentParentPageId, parentParentPage);
                  nonLeafType* parentParentPageData = reinterpret_cast<nonLeafType*>(parentParentPage);
                  nonLeafType* leftPageData = reinterpret_cast<nonLeafType*>(pLeftSib);
#ifdef DEBUG 
                  assert(parentParentPageOffset >= 1);
#endif
                  traits::assign(parentPageData->keyArray[0], parentParentPageData->keyArray[parentParentPageOffset-1]);
                  traits::assign(parentParentPageData->keyArray[parentParentPageOffset-1], leftPageData->keyArray[pLeftOccupancy-2]);
                  traits::assign(leftPageData->keyArray[pLeftOccupancy-2], 0);
                  this->bufMgr->unPinPage(this->file, parentParentPageId, true);
#ifdef DEBUG 
                  std::cout << "Parent non-leaf node rotated right." << std::endl;
#endif
                } else {
#ifdef DEBUG 
                  assert(0);
#endif
                }
              } else if (pLeftOccupancy != -1) {
                if (parentPageOffset+1 <= traits::NONLEAFSIZE)this->deleteEntryInNonLeaf<keyType, traits>(parentPage, parentPageOffset+1, parentOccupancy);
                else {
#ifdef DEBUG 
                  assert(0);
#endif
                }
                nonLeafType* leftPageData = reinterpret_cast<nonLeafType*>(pLeftSib);
                for (int i = pLeftOccupancy,j=1 ; i < traits::NONLEAFSIZE; i++, j++) {
                  traits::assign(leftPageData->keyArray[i],parentPageData->keyArray[j-1]);
                  leftPageData->pageNoArray[i] = parentPageData->pageNoArray[j-1];
                }
                pathOfTraversal.pop_back();
                if (!pathOfTraversal.empty()) {
                  PageId parentParentPageOffset = pathOfTraversal.back().first;
                  int parentParentPageId = pathOfTraversal.back().second;
                  Page* parentParentPage;
                  this->bufMgr->readPage(this->file, parentParentPageId, parentParentPage);
                  nonLeafType* parentParentPageData = reinterpret_cast<nonLeafType*>(parentParentPage);
#ifdef DEBUG 
                  assert(parentParentPageOffset >= 1);
#endif
                  traits::assign(leftPageData->keyArray[pLeftOccupancy-1], parentParentPageData->keyArray[parentParentPageOffset-1]);
                  this->bufMgr->unPinPage(this->file, parentParentPageId, true);
#ifdef DEBUG 
                  std::cout << "Parent non-leaf node merged with left." << std::endl;
#endif
                } else {
#ifdef DEBUG 
                  assert(0);
#endif
                }
              } else if (pRightOccupancy != -1) {
                if (parentPageOffset+1 <= traits::NONLEAFSIZE)this->deleteEntryInNonLeaf<keyType, traits>(parentPage, parentPageOffset+1, parentOccupancy);
                else {
#ifdef DEBUG 
                  assert(0);
#endif
                }
                parentOccupancy-= 1;
                nonLeafType* rightPageData = reinterpret_cast<nonLeafType*>(pRightSib);
                for (int i = parentOccupancy,j = 0; i < traits::NONLEAFSIZE; j++,i++) {
                  traits::assign(parentPageData->keyArray[i],rightPageData->keyArray[j]);
                  parentPageData->pageNoArray[i] = rightPageData->pageNoArray[j];
                }
                pathOfTraversal.pop_back();
                if (!pathOfTraversal.empty()) {
                  PageId parentParentPageOffset = pathOfTraversal.back().first;
                  int parentParentPageId = pathOfTraversal.back().second;
                  Page* parentParentPage;
                  this->bufMgr->readPage(this->file, parentParentPageId, parentParentPage);
                  nonLeafType* parentParentPageData = reinterpret_cast<nonLeafType*>(parentParentPage);
#ifdef DEBUG
                  assert(parentParentPageOffset>=0);
#endif
                  traits::assign(parentPageData->keyArray[parentOccupancy-1], parentParentPageData->keyArray[parentParentPageOffset]);
                  this->bufMgr->unPinPage(this->file, parentParentPageId, true);
#ifdef DEBUG 
                  std::cout << "Parent non-leaf node merged with right." << std::endl;
#endif
                } else {
#ifdef DEBUG 
                  assert(0);
#endif
                }
              } else {
#ifdef DEBUG 
                std::cout << "Non-leaf page made root." << std::endl;
#endif
                if (parentPageOffset+1 <= traits::NONLEAFSIZE)this->deleteEntryInNonLeaf<keyType, traits>(parentPage, parentPageOffset+1, parentOccupancy);
                else {
#ifdef DEBUG 
                  assert(0);
#endif
                }
                this->rootPageNum = parentPageId;
                if (leftOccupancy != -1) this->bufMgr->unPinPage(this->file, leftSibling, true);
                if (rightOccupancy != -1) this->bufMgr->unPinPage(this->file, rightSibling, true);
                this->bufMgr->unPinPage(this->file, parentPageId, true);
                break;
              }
              if(-1 != pLeftOccupancy) this->bufMgr->unPinPage(this->file, pLeftSibling, true);
              if(-1 != pRightOccupancy) this->bufMgr->unPinPage(this->file, pRightSibling, true);
              this->bufMgr->unPinPage(this->file, parentPageId, true);
            }
            if (true == rotated) {
              if(-1 != leftOccupancy) this->bufMgr->unPinPage(this->file, leftSibling, true);
              if(-1 != rightOccupancy) this->bufMgr->unPinPage(this->file, rightSibling, true);
              break;
            }
          }
#ifdef DEBUG 
          std::cout << "merging with right page." << std::endl;
#endif
        } else {
          this->deleteEntryInLeaf<keyType, traits>(reinterpret_cast<Page*>(dataPage), startLoc, endLoc);
          this->bufMgr->unPinPage(this->file, dataPageId, true);
#ifdef DEBUG 
          std::cout << "No left/right page, just deleted." << std::endl;
#endif
        }
      } else {
        this->deleteEntryInLeaf<keyType, traits>(reinterpret_cast<Page*>(dataPage), startLoc, endLoc);
        this->bufMgr->unPinPage(this->file, dataPageId, true);
        this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
      }
    }
  }
#ifdef DEBUG
  assert(retval);
  this->bufMgr->flushFile(this->file);
#endif
#ifdef DEBUG
  if (this->attributeType != STRING) {
    bool rc = this->isStructureValid<keyType,traits>();
    assert(true == rc);
  }
#endif
    return retval;
}

}
