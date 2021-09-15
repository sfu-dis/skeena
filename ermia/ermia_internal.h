#pragma once
#include <map>
#include "dbcore/sm-common.h"
#include "masstree/masstree_btree_noxid.h"

namespace ermia {

// Base class for user-facing index implementations
class OrderedIndex {
  friend class transaction;

protected:
  TableDescriptor *table_descriptor;
  bool is_primary;
  bool is_unique = false;
  FID self_fid;

public:
  OrderedIndex(std::string table_name, bool is_primary);
  virtual ~OrderedIndex() {}
  inline TableDescriptor *GetTableDescriptor() { return table_descriptor; }
  inline bool IsPrimary() { return is_primary; }
  inline bool IsUnique() { return is_unique; }
  inline FID GetIndexFid() { return self_fid; }
  virtual void *GetTable() = 0;
  // TODO: Support unique secondary index.
  inline void SetUnique(bool uk) {is_unique = uk;}

  class ScanCallback {
  public:
    size_t limit = -1;
    virtual ~ScanCallback() {}
    virtual bool Invoke(const char *keyp, size_t keylen,
                        const varstr &value) = 0;
  };

  // Get a record with a key of length keylen. The underlying DB does not manage
  // the memory associated with key. [rc] stores TRUE if found, FALSE otherwise.
  virtual void GetRecord(transaction *t, rc_t &rc, const varstr &key, varstr &value,
                                  OID *out_oid = nullptr) = 0;

  // Return the OID that corresponds the given key
  virtual void GetOID(const varstr &key, rc_t &rc, TXN::xid_context *xc, OID &out_oid,
                               ConcurrentMasstree::versioned_node_t *out_sinfo = nullptr) = 0;

  // Update a database record with a key of length keylen, with mapping of length
  // valuelen.  The underlying DB does not manage the memory pointed to by key or
  // value (a copy is made).
  //
  // If the does not already exist and config::upsert is set to true, insert.
  virtual rc_t UpdateRecord(transaction *t, const varstr &key, varstr &value) = 0;

  // Insert a record with a key of length keylen.
  virtual rc_t InsertRecord(transaction *t, const varstr &key, varstr &value,
                                     OID *out_oid = nullptr) = 0;

  // Map a key to an existing OID. Could be used for primary or secondary index.
  // TODO: Insert a key to an existing OID, this key can contain multiple OIDs
  virtual bool InsertOID(transaction *t, const varstr &key, OID oid) = 0;

  // Search [start_key, *end_key) if end_key is not null, otherwise
  // search [start_key, +infty)
  virtual rc_t Scan(transaction *t, const varstr &start_key,
                             const varstr *end_key, ScanCallback &callback,
                             str_arena *arena) = 0;
  // Search (*end_key, start_key] if end_key is not null, otherwise
  // search (-infty, start_key] (starting at start_key and traversing
  // backwards)
  virtual rc_t ReverseScan(transaction *t, const varstr &start_key,
                                    const varstr *end_key, ScanCallback &callback,
                                    str_arena *arena) = 0;

  // Default implementation calls put() with NULL (zero-length) value
  virtual rc_t RemoveRecord(transaction *t, const varstr &key) = 0;

  virtual size_t Size() = 0;
  virtual std::map<std::string, uint64_t> Clear() = 0;
  virtual void SetArrays(bool) = 0;

  /**
   * Insert key-oid pair to the underlying actual index structure.
   *
   * Returns false if the record already exists or there is potential phantom.
   */
  virtual bool InsertIfAbsent(transaction *t, const varstr &key, OID oid) = 0;
};

}  // namespace ermia
