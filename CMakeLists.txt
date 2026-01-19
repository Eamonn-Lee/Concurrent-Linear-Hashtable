cmake_minimum_required(VERSION 3.1...3.5)
project(MVCC_Linear_hashtable)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED YES)
set(CMAKE_CXX_EXTENSIONS NO)

add_compile_options(
        -Wall
        -Wextra
#        -Werror        # rmv werror to not fail ci builds
        -pedantic-errors
        -Wconversion
        -Wsign-conversion
        -Wdouble-promotion
        -Wcast-align
        -Wformat=2
        -Wuninitialized
        -Wnull-dereference
        -Wnon-virtual-dtor
        -Woverloaded-virtual
        -Wdeprecated-copy-dtor
        -Wold-style-cast
        -Wzero-as-null-pointer-constant
        -Wsuggest-override
        -fstack-protector-strong
)

# debug builds should be compiled with sanitizers
# sanitizers are small libraries that check things like buffer overrun with minimal runtime overhead.
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address,undefined -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fsanitize=address,undefined -fno-omit-frame-pointer")
set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} -fsanitize=address,undefined")
set(CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO "${CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO} -fsanitize=address,undefined")

# add the testing library Catch2
enable_testing()
add_library(catch2_main lib/catch2_main.cpp)
target_include_directories(catch2_main SYSTEM PUBLIC lib)
target_compile_options(catch2_main PRIVATE -w)
# link the library so that other programs can get it

# Changed to a library because header files cannot be executables
add_library(MVCC_Linear_hashtable INTERFACE)
target_include_directories(MVCC_Linear_hashtable INTERFACE src)

# link testing
add_executable(linear_hash_test_exe src/linear_hash.test.cpp)
target_link_libraries(linear_hash_test_exe PRIVATE MVCC_Linear_hashtable catch2_main)
add_test(linear_hash_test linear_hash_test_exe)