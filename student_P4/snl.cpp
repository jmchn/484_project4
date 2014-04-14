#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <cstring>
Status Operators::SNL(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
  cout << "Algorithm: Simple NL Join" << endl;

  /* Your solution goes here */
  Status Scanattr1;
  HeapFileScan *FindMatch1;
  FindMatch1 = new HeapFileScan(attrDesc1.relName, Scanattr1);

  if (Scanattr1 != OK){
  	return Scanattr1;
  }



  Status StatusAfterOpenResult;
  HeapFile resultHeapFile(result, StatusAfterOpenResult);
  if ( StatusAfterOpenResult != OK){
  	return StatusAfterOpenResult;
  }

  RID outRid1; 
  Record rec1; 
  Status statusGetNext1; 

  while((statusGetNext1 = FindMatch1->scanNext(outRid1,rec1)) ==OK){
  	Status  Scanattr2;
  	HeapFileScan *FindMatch2;
  	FindMatch2 = new HeapFileScan(attrDesc2.relName, Scanattr2);
  	if (Scanattr2 != OK){
  		return Scanattr2;
  	}

  	RID outRid2;
  	Record rec2;
  	Status statusGetNext2;
  	while ((statusGetNext2 = FindMatch2->scanNext(outRid2,rec2)) ==OK){
  		if (( (op == LT || op==LTE) && matchRec(rec1,rec2,attrDesc1,attrDesc2) < 0) ||
  			(matchRec(rec1,rec2,attrDesc1,attrDesc2) == 0 && (op == LTE || op == GTE)) ||
  			(matchRec(rec1,rec2,attrDesc1,attrDesc2) > 0 && (op == GT || op == GTE)) 
  		){
  			int i;
        			Record resultRec;
        			RID dummy;// no use
        			char* data = new char[reclen];
        			char* temp = data;
        			for( i = 0 ; i < projCnt ; i++)
        			{
        				if ( strcmp(attrDescArray[i].relName, attrDesc1.relName)==0 ){
            					memcpy(temp,(char*)rec1.data+attrDescArray[i].attrOffset,attrDescArray[i].attrLen);
            					temp = temp + attrDescArray[i].attrLen;
            				}
            				else {
            					memcpy(temp,(char*)rec2.data+attrDescArray[i].attrOffset,attrDescArray[i].attrLen);
            					temp = temp + attrDescArray[i].attrLen;
            				}
        			}
        			resultRec.data = (void*) data;
        			resultRec.length = reclen;
        			Status statusInsertRec;
        			statusInsertRec = resultHeapFile.insertRecord(resultRec,dummy);
        			if(statusInsertRec != OK)
        			{
            				return statusInsertRec;
        			}
  		}
  		
  	}

  	if(statusGetNext2 != FILEEOF){
        		return statusGetNext2;
  	}
  	delete FindMatch2;
  }

  if(statusGetNext1 != FILEEOF){
        return statusGetNext1;
  }
  delete FindMatch1;

  return OK;
}

