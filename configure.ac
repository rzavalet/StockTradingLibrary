#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

## Prelude
AC_PREREQ([2.69])
AC_INIT([libstocktrading], [0.1], [rj.zavaleta@gmail.com])
AC_CONFIG_AUX_DIR([build-aux])
AM_INIT_AUTOMAKE([foreign -Wall -Werror subdir-objects])

## Checks for programs
AC_PROG_CC

## Checks for libraries.
AC_CHECK_LIB([db-6.2],[db_env_create], [], [AC_MSG_ERROR(db-6.2 was not found)])
AC_CHECK_LIB([rt], [clock_gettime], [], [AC_MSG_ERROR(rt was not found)])

## Checks for header files.
AC_CHECK_HEADERS([db.h], [], [AC_MSG_ERROR(db.h was not found)])
AC_CHECK_HEADERS([time.h], [], [AC_MSG_ERROR(rt.h was not found)])
#
## Checks for typedefs, structures, and compiler characteristics.
#AC_TYPE_SIZE_T
#
## Checks for library functions.
#AC_FUNC_MALLOC
#AC_CHECK_FUNCS([memset strdup])
#
#
AM_PROG_AR
AC_PROG_RANLIB
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_FILES([Makefile src/Makefile])
AC_OUTPUT
