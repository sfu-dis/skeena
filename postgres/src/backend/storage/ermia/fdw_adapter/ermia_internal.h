#ifndef ERMIA_INTERNAL_H
#define ERMIA_INTERNAL_H

extern "C"
{
#include "ermia_api.h"
}

/* Headers in ERMIA start*/
#include "dbcore/rcu.h"
#include "dbcore/sm-log-recover-impl.h"
#include "dbcore/sm-rc.h"
#include "dbcore/sm-table.h"
#include "str_arena.h"
#include "txn.h"
/* Headers in ERMIA end*/

class ERMIAAdaptor
{
public:
  /**
   * @brief Initialize ERMIA engine singleton.
   */
  static void Init();

  /**
   * @brief Destroy ERMIA engine singleton.
   */
  static void Destroy();

  /** @var indicates if ERMIA engine initialized */
  static bool m_initialized;
};

#endif // ERMIA_INTERNAL_H