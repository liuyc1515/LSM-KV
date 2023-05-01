project(LSMKV)

add_library(liblsmkv STATIC bloomfilter.cpp kvstore.cpp memory.cpp skiplist.cpp sstable.cpp)

target_include_directories(liblsmkv PUBLIC ${CMAKE_SOURCE_DIR}/include)
