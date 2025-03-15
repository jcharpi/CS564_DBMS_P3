#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

//----------------------------------------
// File: buf.c
// Description: Implementation of the buffer manager for the Minirel database system
// This file has the implementation for these classes: BufMgr, BufDesc, BufHashTbl
// These specific methods are implemented: allocBuf, readPage, unPinPage, allocPage
// Authors: Josh Charpentier - 9083576539
//          Rahul Polavarapu - 
//          Mohith Nellivalasa - 9084317024
//----------------------------------------

#define ASSERT(c)                                        \
  {                                                      \
    if (!(c))                                            \
    {                                                    \
      cerr << "At line " << __LINE__ << ":" << endl      \
           << "  ";                                      \
      cerr << "This condition should hold: " #c << endl; \
      exit(1);                                           \
    }                                                    \
  }

//----------------------------------------
// Constructor of the class BufMgr
// Initializes buffer manager with the given number of buffers
// Input: int bufs - number of buffers to initialize
// Output: None
// Return: None
//----------------------------------------
BufMgr::BufMgr(const int bufs)
{
  numBufs = bufs;

  bufTable = new BufDesc[bufs];
  memset(bufTable, 0, bufs * sizeof(BufDesc));
  for (int i = 0; i < bufs; i++)
  {
    bufTable[i].frameNo = i;
    bufTable[i].valid = false;
  }

  bufPool = new Page[bufs];
  memset(bufPool, 0, bufs * sizeof(Page));

  int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
  hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

  clockHand = bufs - 1;
}

//----------------------------------------
// Destructor of the class Bufmgr
// This section cleans up the buffer manager while flushing any dirty pages to disk
// Input: None
// Output: None
// Return: None
//----------------------------------------
BufMgr::~BufMgr()
{

  // flush out all unwritten pages
  for (int i = 0; i < numBufs; i++)
  {
    BufDesc *tmpbuf = &bufTable[i];
    if (tmpbuf->valid == true && tmpbuf->dirty == true)
    {

#ifdef DEBUGBUF
      cout << "flushing page " << tmpbuf->pageNo
           << " from frame " << i << endl;
#endif

      tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
    }
  }

  delete[] bufTable;
  delete[] bufPool;
}

//----------------------------------------
// Allocates a buffer frame for a page using the clock algorithm.
// This method is also called on by readPage() and allocPage()
// Input: frame - A reference to an integer to store the allocated frame number
// Output: frame - Allocated frame number
// Return: Status - OK if successful,
//                  BUFFEREXCEEDED if no frames are available,
//                  UNIXERR if an error occurred while writing a dirty page to disk
//----------------------------------------
const Status BufMgr::allocBuf(int &frame)
{
  // storing initial hand position to compare with hand position at later times
  int frameCount = 0;

  // Looping through clock, firstly considering all cases where !refbit.
  // Second pass ensures that frames with refbit cleared in the first pass are then checked too.
  while (frameCount < 2 * numBufs)
  {
    // Frame state details for frame at current clockHand
    BufDesc *frameState = &bufTable[clockHand];

    // 1. if frame is invalid, set() on frame
    if (!frameState->valid)
    {
      frame = clockHand;
      advanceClock();
      return OK;
    }

    // 2. if valid AND if refbit, clear refbit, advance clock
    if (frameState->refbit)
    {
      frameState->refbit = false;
      frameCount++;
      advanceClock();
      continue;
    }

    // 3. if valid AND !refbit AND check pinCnt > 0, advance clock
    if (frameState->pinCnt > 0)
    {
      frameCount++;
      advanceClock();
      continue;
    }

    // 4. if valid AND !refbit AND pinCnt == 0 AND dirty, flush page to disk => clear old frame from Hashtable, set() on frame
    if (frameState->dirty)
    {
      // Status of flushing page to disc
      Status stat = frameState->file->writePage(frameState->pageNo, &bufPool[clockHand]);
      if (stat != OK)
        return UNIXERR; // Couldn't flush page to disc
      frameState->dirty = false;
    }

    // 5. if valid AND !refbit AND pinCnt == 0 AND !dirty, clear old frame from Hashtable, set() on frame
    hashTable->remove(frameState->file, frameState->pageNo); // remove file from hashtable
    frameState->Clear();                                     // clear frame for new page

    // update frame to position of clock hand (frame free for use)
    frame = clockHand;
    advanceClock();
    return OK;
  }

  // If we've gone through and checked every frame, even those that were originally had refBit set
  // and we still haven't found a frame, they must all be pinned
  return BUFFEREXCEEDED;
}


//----------------------------------------
// Reads a page from disk into the buffer pool based on lookup() call
// Input: file - pointer to the file object
//        PageNo - page number to read
// Output: page - pointer to frame containing the page via page parameter
// Return: Status - OK if successful, 
//                  HASHNOTFOUND if page not found in hash table,
//                  UNIXERR if a Unix error occurred, 
//                  BUFFEREXCEEDED if all buffer frames are pinned,
//                  HASHTBLERROR if a hash table error occurred
//----------------------------------------
const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{
  int frameNo; // updated on lookup() call
  Status stat;

  // 1. Lookup page in hash table
  stat = hashTable->lookup(file, PageNo, frameNo);

  // Set() will leave the pinCnt for the page set to 1
  // Return a pointer to the frame containing the page via the page parameter.

  // 2. Page not in bufferPool
  if (stat == HASHNOTFOUND)
  {
    int frame;

    // 3. Call allocBuf() to allocate a buffer frame
    stat = allocBuf(frame);
    if (stat != OK)
      return stat;

    // 4. Call the method file->readPage() to read the page from disk into the buffer pool frame
    stat = file->readPage(PageNo, &bufPool[frame]);
    if (stat != OK)
    {
      bufTable[frame].Clear(); // Clear the invalid frame
      return stat;
    }

    // 5. Insert the page into the hashtable
    stat = hashTable->insert(file, PageNo, frame);
    if (stat != OK)
    {
      bufTable[frame].Clear();
      return HASHTBLERROR;
    }

    // 6. Invoke Set() on the frame to set it up properly
    bufTable[frame].Set(file, PageNo);
    page = &bufPool[frame];

    return OK;
  }
  // 7. Page in bufferPool
  else if (stat == OK)
  {
    BufDesc *frameState = &bufTable[frameNo];
    // 8. Set the appropriate refbit
    frameState->refbit = true;

    // 9. Increment the pinCnt for the page
    frameState->pinCnt++;

    // 10. Return a pointer to the frame containing the page via the page parameter
    page = &bufPool[frameNo];

    // 11. Returns OK if no errors occurred
    return OK;
  }

  return stat;
  // 12. UNIXERR if a Unix error occurred
  // 13. BUFFEREXCEEDED if all buffer frames are pinned
  // 14. HASHTBLERROR if a hash table error occurred
}

//----------------------------------------
// Unpins a page from the buffer pool by decrementing pinCnt, if page is dirty
// this method will set the dirty bit as well.
// Input: file - pointer to the file object
//        PageNo - page number to unpin
//        dirty - boolean indicating if the page is dirty
// Output: None
// Return: Status - OK if successful, 
//                  HASHNOTFOUND if page not found in hash table,
//                  PAGENOTPINNED if page is not pinned (pinCnt=0)
//----------------------------------------
const Status BufMgr::unPinPage(File *file, const int PageNo,
                               const bool dirty)
{
  int frameNo; // updated on lookup() call
  Status stat;

  // 1. Lookup page in hash table
  stat = hashTable->lookup(file, PageNo, frameNo);

  // 2. If page not in bufferPool, return HASHNOTFOUND
  if (stat != OK)
    return HASHNOTFOUND;

  // Now we know we have valid frame (2), therefore we can get frameState
  BufDesc *frameState = &bufTable[frameNo];

  // 3. If frame not pinned anywhere, return PAGENOTPINNED
  if (frameState->pinCnt == 0)
    return PAGENOTPINNED;

  // 4. Decrement pinCnt
  frameState->pinCnt--;

  // 5. Set dirty bit if dirty param == true
  if (dirty)
    frameState->dirty = true;

  // 6. Returns OK if no errors occurred
  return OK;
}

//----------------------------------------
// Allocates a new page and a buffer frame for it. Will also insert entry into
// and invoke Set() on frame
// Input: file - pointer to the file object
//        pageNo - reference to an integer to store the allocated page number
// Output: pageNo - newly allocated page's page number
//         page - pointer to frame containing the page via page parameter
// Return: Status - OK if successful,
//                  UNIXERR if a Unix error occurred, 
//                  BUFFEREXCEEDED if all buffer frames are pinned,
//                  HASHTBLERROR if a hash table error occurred. 
//----------------------------------------
const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page)
{
 int frameNo;
  Status stat;
  
  // 1. Allocate the page
  stat = file->allocatePage(pageNo);
  if(stat != OK){
    // 2. Return UNIXERR if a Unix error occurred
    return stat;
  }

  // 3. Allocate a buffer frame for the page
  stat = allocBuf(frameNo);
  if(stat != OK){
    // 4. Return BUFFEREXCEEDED if all buffer frames are pinned
    return stat;
  }

  // 5. Map the frame to the hashtable
  stat = hashTable->insert(file, pageNo, frameNo);
  if(stat != OK){
    // 6. Return HASHTBLERROR if a hash table error occurred
    return stat;
  }

  // 7. Set page from disc into buf frame
  bufTable[frameNo].Set(file, pageNo);
  bufTable[frameNo].frameNo = frameNo;
  page = &bufPool[frameNo];

  // 8. Returns OK if no errors occurred
  return OK;
}

//----------------------------------------
// Disposes of a page from the buffer pool and the file.
// Input: file - pointer to the file object
//        pageNo - page number to dispose
// Output: None
// Return: Status - OK if successful,
//                  error code otherwise 
//----------------------------------------
const Status BufMgr::disposePage(File *file, const int pageNo)
{
  // 1. See if page is in the buffer pool
  Status status = OK;
  int frameNo = 0;
  status = hashTable->lookup(file, pageNo, frameNo);
  if (status == OK)
  {
    // 2. Clear the page
    bufTable[frameNo].Clear();
  }
  status = hashTable->remove(file, pageNo);

  // 3. Deallocate page in the file
  return file->disposePage(pageNo);
}

//----------------------------------------
// Flushes all pages of a file from the buffer pool to disk
// Input: file - pointer to the file object
// Output: None
// Return: Status - OK if successful,
//                  PAGEPINNED if any page is pinned,
//----------------------------------------
const Status BufMgr::flushFile(const File *file)
{
  Status status;

  for (int i = 0; i < numBufs; i++)
  {
    BufDesc *tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file)
    {

      if (tmpbuf->pinCnt > 0)
        return PAGEPINNED;

      if (tmpbuf->dirty == true)
      {
#ifdef DEBUGBUF
        cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
        if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
                                              &(bufPool[i]))) != OK)
          return status;

        tmpbuf->dirty = false;
      }

      hashTable->remove(file, tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }

  return OK;
}

//----------------------------------------
// Prints the current state of the buffer pool.
// Input: None
// Output: None
// Return: None
//----------------------------------------
void BufMgr::printSelf(void)
{
  BufDesc *tmpbuf;

  cout << endl
       << "Print buffer...\n";
  for (int i = 0; i < numBufs; i++)
  {
    tmpbuf = &(bufTable[i]);
    cout << i << "\t" << (char *)(&bufPool[i])
         << "\tpinCnt: " << tmpbuf->pinCnt;

    if (tmpbuf->valid == true)
      cout << "\tvalid\n";
    cout << endl;
  };
}
