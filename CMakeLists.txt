cmake_minimum_required(VERSION 3.20)

project(FileClient LANGUAGES CXX)

find_package(msquic CONFIG REQUIRED)

add_library(FileClient STATIC
    src/Client.cpp
    src/Connection.cpp
    src/Error.cpp
    src/MsQuic.cpp
    src/PacketSender.cpp
    src/FileReader.cpp
    src/FileTransfer.cpp
    src/PacketReceiver.cpp
)

target_compile_features(FileClient PUBLIC cxx_std_17)

target_include_directories(FileClient
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(FileClient
    PUBLIC
        msquic
)
