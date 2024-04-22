cmake_minimum_required(VERSION 3.8)
project(zenoh_pico_example)

set(CMAKE_BUILD_TYPE DEBUG)

include(FetchContent)
FetchContent_declare(
  c_backend 
  GIT_REPOSITORY "https://github.com/eclipse-zenoh/zenoh-pico.git" 
  GIT_TAG main)
set(BUILD_SHARED_LIBS OFF)
FetchContent_MakeAvailable(c_backend)

include(FetchContent)
FetchContent_declare(
  cyclonedds
  GIT_REPOSITORY "https://github.com/eclipse-cyclonedds/cyclonedds.git" 
  GIT_TAG origin/master
  SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/cyclonedds"
  SOURCE_SUBDIR src/core/cdr
)
set(BUILD_SHARED_LIBS OFF)
set(CMAKE_BUILD_TYPE DEBUG)

FetchContent_MakeAvailable(cyclonedds)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/idlc)

add_executable(add_two_ints_client
  add_two_ints_client.c
  idlc/example_interfaces/srv/AddTwoInts.c
)
target_link_libraries(add_two_ints_client
  zenohpico
  cdr
)

add_executable(listener
  listener.c
  idlc/rcl_interfaces/msg/Log.c
  idlc/builtin_interfaces/msg/Time.c
)

target_link_libraries(listener
  zenohpico
  cdr
)

add_executable(talker
  talker.c
  idlc/rcl_interfaces/msg/Log.c
  idlc/builtin_interfaces/msg/Time.c
)

target_link_libraries(talker
  zenohpico
  cdr
)

install(TARGETS add_two_ints_client listener talker
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)


