#ifndef __ZK_COMMON_H__
#define __ZK_COMMON_H__

#include "ZkJPoolAdaptor.h"
#include "CwxGetOpt.h"
#include "CwxTimeValue.h"
#include "CwxFile.h"

CWINUX_USING_NAMESPACE

void output(FILE* fd, int result, int zkstate, char const* format, char const* msg);


#endif 
