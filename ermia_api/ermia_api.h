#ifndef ERMIA_API_H
#define ERMIA_API_H

#ifdef __cplusplus
extern "C"
{
#endif

#include "postgres.h"
#include "access/attnum.h"

  extern void ERMIAAdaptorInit();
  extern void ERMIAAdaptorDestroy();
  extern bool ERMIAAdaptorExists();

#ifdef __cplusplus
}
#endif

#endif /* ERMIA_API_H_ */
