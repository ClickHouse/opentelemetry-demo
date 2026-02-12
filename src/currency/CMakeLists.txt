cmake_minimum_required(VERSION 3.1)
project(currency)

find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)

set(GENERATED_PROTOBUF_PATH "${CMAKE_BINARY_DIR}/generated/proto")

add_subdirectory(genproto)

include_directories("${GENERATED_PROTOBUF_PATH}")

add_executable(currency src/server.cpp)
add_dependencies(currency demo-proto)
target_link_libraries(
    currency demo-proto protobuf::libprotobuf
     gRPC::grpc++)

install(TARGETS currency DESTINATION bin)
