dummy_fdw
=========

A readable, null foreign data wrapper for Postgresql 9.3+

This is a simple FDW that is one step above a null data wrapper.
You can go a 

  create foreign data wrapper dummy

This allows for DDL to run without error. It does not allow for a select to
any foreign tables using this FDW. 


