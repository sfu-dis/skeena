#ifndef ERMIA_API_H
#define ERMIA_API_H

#ifdef __cplusplus
extern "C"
{
#endif

  #include <stdbool.h>

  extern void AdaptorInit();
  extern void AdaptorDestroy();
  extern bool AdaptorExists();
  extern void AdaptorCreateTable(const char *schemaname, const char *relname);

#ifdef __cplusplus
}
#endif

#endif /* ERMIA_API_H_ */
