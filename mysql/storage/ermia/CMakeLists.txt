# Copyright (c) 2006, 2017, Oracle and/or its affiliates. All rights reserved.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have included with MySQL.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

SET(ERMIA_PLUGIN_DYNAMIC "ha_ermia")
SET(ERMIA_SOURCES ha_ermia.cc)
ADD_DEFINITIONS(-DMYSQL_SERVER)
IF(WITH_ERMIA_STORAGE_ENGINE AND NOT WITHOUT_ERMIA_STORAGE_ENGINE)
  MYSQL_ADD_PLUGIN(ermia ${ERMIA_SOURCES} STORAGE_ENGINE DEFAULT)
ELSEIF(NOT WITHOUT_ERMIA_STORAGE_ENGINE)
  MYSQL_ADD_PLUGIN(ermia ${ERMIA_SOURCES} STORAGE_ENGINE MODULE_ONLY)
ENDIF()

IF(NOT (${ERMIA_HEADER_PATH} STREQUAL ""))
  if(${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -include ${ERMIA_HEADER_PATH}/masstree/config-debug.h")
  else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -include ${ERMIA_HEADER_PATH}/masstree/config-release.h")
  endif()

  include_directories(${ERMIA_HEADER_PATH})
  include_directories(${ERMIA_HEADER_PATH}/masstree)
  include_directories(${ERMIA_HEADER_PATH}/third-party/sparsehash/src)
ENDIF()
