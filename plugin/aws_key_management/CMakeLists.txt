# We build parts of AWS C++ SDK as CMake external project
# The restrictions of the SDK (https://github.com/awslabs/aws-sdk-cpp/blob/master/README.md)
# are
 
# - OS : Windows,Linux or OSX
# - C++11 compiler : VS2013+, gcc 4.8+, clang 3.3+
# - libcurl development package needs to be present on Unixes
#
# If we build SDK outselves, we'll need require GIT to be present on the build machine


# Give message why the building this plugin is skipped (only if -DVERBOSE is defined)
# or if plugin is explicitly requested to build. Then bail out.
MACRO(SKIP_AWS_PLUGIN msg)
  IF(VERBOSE OR "${PLUGIN_AWS_KEY_MANAGEMENT}" MATCHES "^(STATIC|DYNAMIC)$")
    MESSAGE(STATUS "Can't build aws_key_management - ${msg}")
  ENDIF()
  IF(TARGET aws_key_management)
    MESSAGE(FATAL_ERROR "Error configuring aws_key_management - aborting")
  ENDIF()
  RETURN()
ENDMACRO()

MYSQL_ADD_PLUGIN(aws_key_management aws_key_management_plugin.cc DISABLED
                 COMPONENT aws-key-management)

IF(NOT TARGET aws_key_management)
  RETURN()
ENDIF()

# This plugin needs recent C++ compilers (AWS C++ SDK header files are using C++11 features)
SET(CXX11_FLAGS)
SET(OLD_COMPILER_MSG "AWS SDK requires c++11 -capable compiler (minimal supported versions are g++ 4.8, clang 3.3, VS2103)")

IF(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
  EXECUTE_PROCESS(COMMAND ${CMAKE_CXX_COMPILER} -dumpversion OUTPUT_VARIABLE GCC_VERSION)
  IF (GCC_VERSION VERSION_LESS 4.8)
    SKIP_AWS_PLUGIN("${OLD_COMPILER_MSG}")
  ENDIF()
  SET(CXX11_FLAGS "-std=c++11")
ELSEIF (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  IF ((CMAKE_CXX_COMPILER_VERSION AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS  3.3) OR
     (CLANG_VERSION_STRING AND CLANG_VERSION_STRING VERSION_LESS  3.3))
    SKIP_AWS_PLUGIN("${OLD_COMPILER_MSG}")
  ENDIF()
  SET(CXX11_FLAGS "-stdlib=libc++")
ELSEIF(MSVC)
  IF (MSVC_VERSION LESS 1800)
    SKIP_AWS_PLUGIN("${OLD_COMPILER_MSG}")
  ENDIF()
ELSE()
  SKIP_AWS_PLUGIN("Compiler not supported by AWS C++ SDK")
ENDIF()

IF (NOT(WIN32 OR APPLE  OR (CMAKE_SYSTEM_NAME MATCHES "Linux")))
  SKIP_AWS_PLUGIN("OS unsupported by AWS SDK")
ENDIF()



FIND_LIBRARY(AWS_CPP_SDK_CORE NAMES aws-cpp-sdk-core PATH_SUFFIXES  "${SDK_INSTALL_BINARY_PREFIX}")
FIND_LIBRARY(AWS_CPP_SDK_KMS NAMES aws-cpp-sdk-core PATH_SUFFIXES  "${SDK_INSTALL_BINARY_PREFIX}")
SET(CMAKE_REQUIRED_FLAGS ${CXX11_FLAGS})
CHECK_INCLUDE_FILE_CXX(aws/kms/KMSClient.h HAVE_AWS_HEADERS)

IF(AWS_CPP_SDK_CORE AND AWS_CPP_SDK_KMS AND HAVE_AWS_HEADERS)
  # AWS C++ SDK installed 
  SET(AWS_SDK_LIBS ${AWS_CPP_SDK_CORE} ${AWS_CPP_SDK_KMS})
ELSE()
  # Build from source, using ExternalProject_Add
  # AWS C++ SDK requires cmake 2.8.12
  IF(CMAKE_VERSION VERSION_LESS "2.8.12")
    SKIP_AWS_PLUGIN("CMake is too old")
  ENDIF()
  FIND_PACKAGE(Git)
  IF(NOT GIT_FOUND)
    SKIP_AWS_PLUGIN("no GIT")
  ENDIF()
  INCLUDE(ExternalProject)
  IF(UNIX)
    FIND_PACKAGE(CURL)
    IF(NOT CURL_FOUND)
      SKIP_AWS_PLUGIN("AWS C++ SDK requires libcurl development package")
    ENDIF()
    SET(PIC_FLAG -fPIC)
    FIND_PATH(UUID_INCLUDE_DIR uuid/uuid.h)
    IF(NOT UUID_INCLUDE_DIR)
      SKIP_AWS_PLUGIN("AWS C++ SDK requires uuid development package")
    ENDIF()
    IF(NOT APPLE)
      FIND_LIBRARY(UUID_LIBRARIES uuid)
      IF(NOT UUID_LIBRARIES)
        SKIP_AWS_PLUGIN("AWS C++ SDK requires uuid development package")
      ENDIF()
    ENDIF()
  ENDIF()
  IF(MSVC)
    SET(EXTRA_SDK_CMAKE_FLAGS -DCMAKE_CXX_FLAGS_DEBUGOPT="" -DCMAKE_EXE_LINKER_FLAGS_DEBUGOPT="" "-DCMAKE_CXX_FLAGS=/wd4530 /WX-")
  ENDIF()
  IF(CMAKE_CXX_COMPILER)
   SET(EXTRA_SDK_CMAKE_FLAGS ${EXTRA_SDK_CMAKE_FLAGS} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER})
  ENDIF()

  SET(AWS_SDK_PATCH_COMMAND )
  ExternalProject_Add(
    aws_sdk_cpp
    GIT_REPOSITORY "https://github.com/awslabs/aws-sdk-cpp.git"
    GIT_TAG "1.0.8"
    UPDATE_COMMAND ""
    SOURCE_DIR "${CMAKE_BINARY_DIR}/aws-sdk-cpp"
    CMAKE_ARGS 
    -DBUILD_ONLY=kms
    -DBUILD_SHARED_LIBS=OFF
    -DFORCE_SHARED_CRT=OFF
    "-DCMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG} ${PIC_FLAG}"
    "-DCMAKE_CXX_FLAGS_RELWITHDEBINFO=${CMAKE_CXX_FLAGS_RELWITHDEBINFO} ${PIC_FLAG}"
    "-DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE} ${PIC_FLAG}"
    "-DCMAKE_CXX_FLAGS_MINSIZEREL=${CMAKE_CXX_FLAGS_MINSIZEREL} ${PIC_FLAG}"
    ${EXTRA_SDK_CMAKE_FLAGS}
    -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/aws_sdk_cpp
    TEST_COMMAND ""
  )
  SET_TARGET_PROPERTIES(aws_sdk_cpp PROPERTIES EXCLUDE_FROM_ALL TRUE)
  # We do not need to build the whole SDK , just 2 of its libs
  set(AWS_SDK_LIBS aws-cpp-sdk-core aws-cpp-sdk-kms)
  FOREACH(lib ${AWS_SDK_LIBS})
    ADD_LIBRARY(${lib} STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(${lib} aws_sdk_cpp)
    SET(loc "${CMAKE_BINARY_DIR}/aws_sdk_cpp/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${lib}${CMAKE_STATIC_LIBRARY_SUFFIX}")
    SET_TARGET_PROPERTIES(${lib} PROPERTIES IMPORTED_LOCATION ${loc})
    IF(WIN32)
      SET_TARGET_PROPERTIES(${lib} PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES "bcrypt;winhttp;wininet;userenv")
    ELSE()
      SET_TARGET_PROPERTIES(${lib} PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES "${SSL_LIBRARIES};${CURL_LIBRARIES};${UUID_LIBRARIES}")
    ENDIF()  
  ENDFOREACH()

  IF(CMAKE_SYSTEM_NAME MATCHES "Linux")
    # Need whole-archive , otherwise static libraries are not linked
    SET(AWS_SDK_LIBS -Wl,--whole-archive ${AWS_SDK_LIBS} -Wl,--no-whole-archive)
  ENDIF()
  SET_TARGET_PROPERTIES(aws_sdk_cpp PROPERTIES EXCLUDE_FROM_ALL TRUE)
  INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/aws_sdk_cpp/include)
ENDIF()

ADD_DEFINITIONS(${SSL_DEFINES}) # Need to know whether openssl should be initialized
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  ${CXX11_FLAGS}")
TARGET_LINK_LIBRARIES(aws_key_management ${AWS_SDK_LIBS})
