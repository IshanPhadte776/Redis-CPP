cmake_minimum_required(VERSION 3.13)

project(redis-starter-cpp)

file(GLOB_RECURSE SOURCE_FILES src/*.cpp)

set(CMAKE_CXX_STANDARD 23) # Enable the C++23 standard
set(THREADS_PREFER_PTHREAD_FLAG ON)

find_package(Threads REQUIRED)

add_executable(redis ${SOURCE_FILES})

target_link_libraries(redis PRIVATE Threads::Threads)

add_executable(redis_unit_test test/test.cpp)
target_include_directories(redis_unit_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_features(redis_unit_test PRIVATE cxx_std_23)

# TCP tests against a running server: ./build/redis --port 6379 &  ./build/redis_integration_test
add_executable(redis_integration_test test/integration_test.cpp)
target_link_libraries(redis_integration_test PRIVATE Threads::Threads)
target_compile_features(redis_integration_test PRIVATE cxx_std_23)