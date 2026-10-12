cmake_minimum_required(VERSION 3.20)
project(shm_ring_lab VERSION 0.1.0 LANGUAGES CXX)

option(BUILD_BENCHMARKS "Build non-gating benchmark helpers" OFF)

if(WIN32)
    set(SHM_RING_PLATFORM_SOURCE src/platform/backend_windows.cpp)
elseif(UNIX)
    set(SHM_RING_PLATFORM_SOURCE src/platform/backend_posix.cpp)
else()
    message(FATAL_ERROR "shm-ring-lab supports Windows and POSIX platforms only")
endif()

find_package(Threads REQUIRED)

add_library(shm_ring_lab
    src/session.cpp
    ${SHM_RING_PLATFORM_SOURCE})
target_compile_features(shm_ring_lab PUBLIC cxx_std_17)
target_include_directories(shm_ring_lab
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(shm_ring_lab PUBLIC Threads::Threads)
if(UNIX AND NOT APPLE)
    target_link_libraries(shm_ring_lab PUBLIC rt)
endif()
if(MSVC)
    target_compile_options(shm_ring_lab PRIVATE /W4 /permissive-)
else()
    target_compile_options(shm_ring_lab PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_executable(shm_ring_demo examples/shm_ring_demo.cpp)
target_link_libraries(shm_ring_demo PRIVATE shm_ring_lab)

enable_testing()
add_executable(shm_ring_test_peer tests/test_peer.cpp)
target_include_directories(shm_ring_test_peer PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(shm_ring_test_peer PRIVATE shm_ring_lab)
add_executable(shm_ring_tests tests/integration.cpp)
target_link_libraries(shm_ring_tests PRIVATE shm_ring_lab Threads::Threads)
add_test(NAME shm_ring_correctness
    COMMAND shm_ring_tests $<TARGET_FILE:shm_ring_test_peer>)
set_tests_properties(shm_ring_correctness PROPERTIES TIMEOUT 90)

if(BUILD_BENCHMARKS)
    add_executable(shm_ring_bench_peer benchmarks/bench_peer.cpp)
    target_link_libraries(shm_ring_bench_peer PRIVATE shm_ring_lab)
endif()
