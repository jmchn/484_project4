#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <cmath>
#include <cstring>
#include "assert.h"

#define MAX(a,b) ((a) > (b) ? (a) : (b))
#define DOUBLEERROR 1e-07

/*
 * Joins two relations
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Operators::Join(const string& result,           // Name of the output relation 
                       const int projCnt,              // Number of attributes in the projection
    	               const attrInfo projNames[],     // List of projection attributes
    	               const attrInfo* attr1,          // Left attr in the join predicate
    	               const Operator op,              // Predicate operator
    	               const attrInfo* attr2)          // Right attr in the join predicate
{
    /* Your solution goes here */
    // convert the datastructure
    int len = 0;
    int j;
    int attrCnt;
    AttrDesc * projNamesDesc,*attrDesc1,*attrDesc2;
    projNamesDesc = new AttrDesc[projCnt];
    Status statusGetProjectName;
    for(j = 0 ; j <projCnt ; j++)
    {
        AttrDesc * attrInCata,*temp;
        statusGetProjectName = attrCat->getRelInfo(projNames[j].relName,attrCnt,attrInCata);
        if(statusGetProjectName != OK)
        {
            return statusGetProjectName;
        }
        int k ;
        for (k = 0,temp = attrInCata ; k < attrCnt ; k++,temp++)
        {
            assert(strcmp(projNames[j].relName,temp->relName)==0);
            if(strcmp(temp->relName,attr1->relName )==0 && strcmp(temp->attrName,attr1->attrName)==0)
            {
                attrDesc1 = temp;
            }
            if(strcmp(temp->relName,attr2->relName )==0 && strcmp(temp->attrName,attr2->attrName)==0)
            {
                attrDesc2 = temp;
            }
            if(strcmp(temp->attrName, projNames[j].attrName)==0)
            {
                projNamesDesc[j]=*temp;
                len+=temp->attrLen;
            }
        }
    }
    //convertion finish
    if (op == EQ)
    {
        //check whether attr1 or attr2 has a index
        if( attrDesc1->indexed == 1)
        {
            Status statusInl;
            statusInl = INL(result, projCnt,projNamesDesc,*attrDesc2,op,*attrDesc1,len);
            return statusInl;
        }
        else 
        {
            if(attrDesc2->indexed == 1)
            {
                Status statusInl;
                statusInl = INL(result, projCnt,projNamesDesc,*attrDesc1,op,*attrDesc2,len);
                return statusInl;
            }
            else
            {
                Status statusSmj;
                statusSmj = SMJ(result,projCnt,projNamesDesc,*attrDesc1,op,*attrDesc2,len);
                return statusSmj;
            }
        }
    }
    else
    {
        // SNL
        Status statusSnl;
        statusSnl = SNL(result,projCnt,projNamesDesc,*attrDesc1,op,*attrDesc2,len);
        return statusSnl;
    }
}

// Function to compare two record based on the predicate. Returns 0 if the two attributes 
// are equal, a negative number if the left (attrDesc1) attribute is less that the right 
// attribute, otherwise this function returns a positive number.
int Operators::matchRec(const Record& outerRec,     // Left record
                        const Record& innerRec,     // Right record
                        const AttrDesc& attrDesc1,  // Left attribute in the predicate
                        const AttrDesc& attrDesc2)  // Right attribute in the predicate
{
    int tmpInt1, tmpInt2;
    double tmpFloat1, tmpFloat2, floatDiff;

    // Compare the attribute values using memcpy to avoid byte alignment issues
    switch(attrDesc1.attrType)
    {
        case INTEGER:
            memcpy(&tmpInt1, (char *) outerRec.data + attrDesc1.attrOffset, sizeof(int));
            memcpy(&tmpInt2, (char *) innerRec.data + attrDesc2.attrOffset, sizeof(int));
            return tmpInt1 - tmpInt2;

        case DOUBLE:
            memcpy(&tmpFloat1, (char *) outerRec.data + attrDesc1.attrOffset, sizeof(double));
            memcpy(&tmpFloat2, (char *) innerRec.data + attrDesc2.attrOffset, sizeof(double));
            floatDiff = tmpFloat1 - tmpFloat2;
            return (fabs(floatDiff) < DOUBLEERROR) ? 0 : (floatDiff < 0?floor(floatDiff):ceil(floatDiff));
        case STRING:
            return strncmp(
                (char *) outerRec.data + attrDesc1.attrOffset, 
                (char *) innerRec.data + attrDesc2.attrOffset, 
                MAX(attrDesc1.attrLen, attrDesc2.attrLen));
    }

    return 0;
}
