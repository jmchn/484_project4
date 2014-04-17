#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <cstring>

/* Consider using Operators::matchRec() defined in join.cpp
 * to compare records when joining the relations */
  
Status Operators::SMJ(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
  cout << "Algorithm: SM Join" << endl;

  /* Your solution goes here */
  unsigned BytesOfavailablepages;
  // get the 80% of the unpinned pages;
  BytesOfavailablepages = 0.8 *1000 * (bufMgr -> numUnpinnedPages());
  int numberOfLeftTuples, numberOfRightTuples;
  int leftTupleSize = 0;
  int rightTupleSize = 0; 
  int leftAttrCnt, rightAttrCnt;

  AttrDesc *leftAttrInCat, *rightAttrInCat; 
  Status statusAfterGetLeftRelInfo, statusAfterGetRightRelInfo;
  statusAfterGetLeftRelInfo = attrCat->getRelInfo(attrDesc1.relName,leftAttrCnt,leftAttrInCat);
  if (statusAfterGetLeftRelInfo != OK){
    return statusAfterGetLeftRelInfo;
  }
// calculating the tuple length.
  for(int i = 0; i< leftAttrCnt;i++){
    leftTupleSize += leftAttrInCat->attrLen;
    leftAttrInCat ++;
  }
  numberOfLeftTuples = BytesOfavailablepages/leftTupleSize;



  statusAfterGetRightRelInfo = attrCat->getRelInfo(attrDesc2.relName,rightAttrCnt,rightAttrInCat);
  if (statusAfterGetRightRelInfo != OK){
    return statusAfterGetRightRelInfo;
  }

  for (int i=0; i < rightAttrCnt; i++){
    rightTupleSize += rightAttrInCat -> attrLen;
    rightAttrInCat ++;
  }
  numberOfRightTuples = BytesOfavailablepages/rightTupleSize;



  Status StatusAfterOpenResult;
  HeapFile resultHeapFile(result, StatusAfterOpenResult);
  if ( StatusAfterOpenResult != OK){
    return StatusAfterOpenResult;
  }

// constructing sortfile;
  Status statusAfterSortingLeft,statusAfterSortingRight;
  SortedFile *left, *right;
  left = new SortedFile(attrDesc1.relName, attrDesc1.attrOffset,attrDesc1.attrLen,
    (Datatype)attrDesc1.attrType, numberOfLeftTuples ,statusAfterSortingLeft);
  right = new SortedFile(attrDesc2.relName, attrDesc2.attrOffset,attrDesc2.attrLen,
    (Datatype)attrDesc2.attrType, numberOfRightTuples ,statusAfterSortingRight);
  if (statusAfterSortingLeft != OK){
    return statusAfterSortingLeft;
  }

  if (statusAfterSortingRight != OK){
    return statusAfterSortingRight;
  }

  Record leftrec,leftnextrec, rightrec,rightnextrec;
  Status leftnext,rightnext,rightSetMark,rightgotoMark;
  bool SetOrNot=0;
  leftnext = left->next(leftrec);
  rightnext = right->next(rightrec);
  while ((leftnext == OK) && (rightnext  == OK)){
    // examine for the equality
    if (matchRec(leftrec,rightrec,attrDesc1,attrDesc2) < 0){
      leftnext =left -> next(leftrec);
    } 
    else if (matchRec(leftrec,rightrec,attrDesc1,attrDesc2) > 0){
      rightnext = right->next(rightrec);
    } 
    else{
              if (SetOrNot == 0){
                rightSetMark = right -> setMark();
                if(rightSetMark != OK)
                {
                    delete left;
                    delete right;
                    delete leftAttrInCat;
                    delete rightAttrInCat;
                    return rightSetMark;
                }
                SetOrNot = 1;
              }
              int i;
              Record resultRec;
              RID dummy;// no use
              char* data = new char[reclen];
              char* temp = data;
              for( i = 0 ; i < projCnt ; i++)
              {
                if ( strcmp(attrDescArray[i].relName, attrDesc1.relName)==0 ){
                      memcpy(temp,(char*)leftrec.data+attrDescArray[i].attrOffset,attrDescArray[i].attrLen);
                      temp = temp + attrDescArray[i].attrLen;
                    }
                    else {
                      memcpy(temp,(char*)rightrec.data+attrDescArray[i].attrOffset,attrDescArray[i].attrLen);
                      temp = temp + attrDescArray[i].attrLen;
                    }
              }
              resultRec.data = (void*) data;
              resultRec.length = reclen;
              Status statusInsertRec;
              statusInsertRec = resultHeapFile.insertRecord(resultRec,dummy);
              if(statusInsertRec != OK)
              {
                    delete left;
                    delete right;
                    delete leftAttrInCat;
                    delete rightAttrInCat;
                    return statusInsertRec;
              }

              rightnext = right-> next(rightnextrec);
              if(rightnext != FILEEOF)
              {
                  if (matchRec (rightrec, rightnextrec, attrDesc2,attrDesc2) ==  0){
                      // if there are multiple same value in the right hand side
                        rightrec = rightnextrec; 
                  } 
              }
              else {
                  // if there is another record with the same value on the left hand side
                    leftnext = left ->next(leftnextrec);
                    if(leftnext == FILEEOF)
                    {
                        break;
                    }
                    if (matchRec(leftrec,leftnextrec,attrDesc1,attrDesc1) == 0){
                        leftrec = leftnextrec; 
                        rightgotoMark = right -> gotoMark();
                        if(rightgotoMark != OK)
                        {
                            delete left;
                            delete right;
                            delete leftAttrInCat;
                            delete rightAttrInCat;
                            return rightgotoMark;
                        }
                        rightnext = right->next(rightrec);
                    }
                    else {
                       leftrec = leftnextrec;
                       rightrec = rightnextrec;
                       SetOrNot =0;
                    }
              }
    }
  }
 delete left;
 delete right;
 delete leftAttrInCat;
 delete rightAttrInCat;

  return OK;
}
























