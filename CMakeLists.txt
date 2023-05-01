cmake_minimum_required(VERSION 3.5)

project(LSMKV LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_subdirectory(src)

if (LSMKV_CORRECTNESS_TEST)
    add_executable(correctness correctness.cpp)
    target_link_libraries(correctness liblsmkv)
else()
    add_executable(persistence persistence.cpp)
    target_link_libraries(persistence liblsmkv)
endif()
