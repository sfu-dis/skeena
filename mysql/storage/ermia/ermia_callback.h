#pragma once
#include <ermia.h>
#include <cstdio>
#include <vector>
#include "storage/ermia/debug_util.h"
class RangeScanCallback : public ermia::OrderedIndex::ScanCallback {
 private:
  std::vector<ermia::varstr> _result;

 public:
  virtual bool Invoke(const char *keyp, size_t keylen,
                      const ermia::varstr &value) {
    MARK_REFERENCED(keyp);
    MARK_REFERENCED(keylen);
    _result.push_back(value);
    return true;
  }
  std::vector<ermia::varstr> &result() { return _result; }
};
