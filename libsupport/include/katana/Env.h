#ifndef KATANA_LIBSUPPORT_KATANA_ENV_H_
#define KATANA_LIBSUPPORT_KATANA_ENV_H_

#include <string>

#include "katana/config.h"

namespace katana {

/// Return true if the environment variable is set.
///
/// This function simply tests for the presence of an environment variable; in
/// contrast, bool GetEnv(std::string, bool&) checks if the value of the
/// environment variable matches common truthy and falsey values.
KATANA_EXPORT bool GetEnv(const std::string& var_name);

/// Return true if environment variable is set, and extract its value into
/// ret_val parameter.
///
/// \param var_name name of the variable
/// \param[out] ret where to store the value of environment variable
/// \return true if environment variable set and value was successfully parsed;
///   false otherwise
KATANA_EXPORT bool GetEnv(const std::string& var_name, bool* ret);
KATANA_EXPORT bool GetEnv(const std::string& var_name, int* ret);
KATANA_EXPORT bool GetEnv(const std::string& var_name, double* ret);
KATANA_EXPORT bool GetEnv(const std::string& var_name, std::string* ret);

/// Set environment variable
/// \param var_name name of the variable
/// \param[in] val new value
/// \param[in] overwrite if true, and var_name exists, overwrite previous value
/// \return true if env not previously set or successfully overwritten
KATANA_EXPORT bool SetEnv(
    const std::string& var_name, const std::string& val, bool overwrite);
KATANA_EXPORT bool UnsetEnv(const std::string& var_name);

}  // end namespace katana

#endif
