#ifndef TEST_FRAAMEWORK_H_
#define TEST_FRAAMEWORK_H_


#include <sstream>
#include <iostream>


/******************************************************************
                       For Test Framework
*******************************************************************/
#define ASSERT(condition, message)                                             \
  do {                                                                         \
    if (!(condition)) {                                                        \
      std::cerr << "Assertion `" #condition "` failed in " << __FILE__ << ":"  \
                << __LINE__ << " msg: " << message << std::endl;               \
      std::terminate();                                                        \
    }                                                                          \
  } while (false)

# endif

