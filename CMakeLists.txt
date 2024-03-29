cmake_minimum_required(VERSION 3.3)
project(verticaudxutilities)

include(CheckCXXCompilerFlag)
set(CMAKE_REQUIRED_QUIET true)
set(CMAKE_VERBOSE_MAKEFILE OFF)

# Use static libstdc++ if Vertica nodes has older OS than your build box.
# Allowed to use c++17 and c++20
MESSAGE(STATUS "Using static libstdc++")
set(CMAKE_SHARED_LINKER_FLAGS "-static-libstdc++")
set(CMAKE_EXE_LINKER_FLAGS "-static-libstdc++")
# End static libstdc++

Message(STATUS "Using compile flag -D_GLIBCXX_USE_CXX11_ABI=0, lost hope waiting for flag day")
add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)

MESSAGE( STATUS "COMPILER VERSION: " ${CMAKE_CXX_COMPILER_VERSION})
#set(CMAKE_CXX_STANDARD 17) # -std=gnu++17
#MESSAGE(STATUS "Using --c++17")
set(CMAKE_CXX_STANDARD 20) # -std=gnu++20
MESSAGE(STATUS "Using --c++20")

set(CMAKE_POSITION_INDEPENDENT_CODE ON) # set -fPIC

#set(CMAKE_CXX_FLAGS "-Wall -Wextra -Wno-unused-parameter -Wno-cpp -Wno-address-of-packed-member -Wno-packed-not-aligned")
set(CMAKE_CXX_FLAGS "-Wall -Wextra -Wno-unused-parameter -Wno-implicit-fallthrough -Wno-deprecated-copy -Wno-cpp")
#set(CMAKE_CXX_FLAGS "-Wall -Wextra -Wno-unused-parameter -Wno-implicit-fallthrough -Wno-deprecated-copy")
set(CMAKE_CXX_FLAGS_DEBUG "-ggdb")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -ggdb")

#set(VERTICA_SDK_INCLUDE "/opt/vertica/sdk/include")
#set(VERTICA_SDK_INCLUDE "/home/scherepanov/git/vertica/extern/include/verticaudx/verticaudx12031")
set(VERTICA_SDK_INCLUDE "/home/scherepanov/git/vertica/extern/include/verticaudx/verticaudx120414")
#set(VERTICA_SDK_INCLUDE "/home/scherepanov/git/vertica/extern/include/verticaudx/verticaudx1102")
set(VERTICA_SDK_CPP "${VERTICA_SDK_INCLUDE}/Vertica.cpp")

set(VERTICA_LIB_HOME "${CMAKE_SOURCE_DIR}/cpp")

set(LIBRARIES_DYNAMIC_VERTICA
        verticaudxutilities
        )

set(verticaudxutilities_PATHS ${VERTICA_LIB_HOME} ${VERTICA_SDK_INCLUDE})

foreach(LIBNAME ${LIBRARIES_DYNAMIC_VERTICA})
    file(GLOB_RECURSE CPP_FILES ${VERTICA_LIB_HOME}/${LIBNAME}/*.cpp)
    add_library("${LIBNAME}" SHARED "${CPP_FILES}" "${VERTICA_SDK_CPP}")
    target_include_directories("${LIBNAME}" PRIVATE ${${LIBNAME}_PATHS})
endforeach()
