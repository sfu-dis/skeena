#ifndef ADAPTOR_H
#define ADAPTOR_H

#include "../dbcore/rcu.h"
#include "../dbcore/sm-log-recover-impl.h"
#include "../dbcore/sm-rc.h"
#include "../dbcore/sm-table.h"
#include "../str_arena.h"
#include "../txn.h"
#include "ermia_api.h"

namespace fdw {

class Adaptor {
 protected:
  Adaptor() {
    InitERMIA();
  }

  /* Format table name */
  void get_full_table_name(const char *schemaname, const char *relname, char *buf);

  /* Format table name */
  void get_full_index_name(const char *schemaname, const char *relname, char *buf);

  /* Adaptor singleton */
  static Adaptor* m_adaptor;

  /* ERMIA singleton */
  static ermia::Engine* m_engine;

  /* Indicates if the engine is initialized */
  static bool m_initialized;

 public:
  /* Singletons should not be cloneable. */
  Adaptor(Adaptor &other) = delete;

  /* Singletons should not be assignable. */
  void operator=(const Adaptor &) = delete;

  /* Initialize Adaptor. */
  static void Init();

  /* Get the singleton of Adaptor. */
  static Adaptor *GetInstance();

  /* Destroy Adaptor. */
  static void Destroy();

  /* If Adaptor exists. */
  static bool Exists();

  /* Initialize ERMIA. */
  static void InitERMIA();

  /* Destroy ERMIA. */
  static void DestroyERMIA();

  /* Get the singleton of ERMIA. */
  static ermia::Engine *GetERMIA();

  /* Create ERMIA table from PG create table statement. */
  void CreateERMIATable(const char *schemaname, const char *relname);
};

}  // namespace fdw
#endif  // ADAPTOR_H