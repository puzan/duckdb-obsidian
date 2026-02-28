cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME obsidian)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/obsidian_extension.cpp
    src/obsidian_frontmatter.cpp
    src/obsidian_wikilinks.cpp
    src/obsidian_body.cpp
)

find_package(cmark-gfm CONFIG REQUIRED)
find_package(ryml CONFIG REQUIRED)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} libcmark-gfm_static ryml::ryml)
target_link_libraries(${LOADABLE_EXTENSION_NAME} libcmark-gfm_static ryml::ryml)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
