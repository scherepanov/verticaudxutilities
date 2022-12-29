#pragma once

#include "Vertica.h"

namespace VerticaUDxUtilities {

class UDxUtilities {
  public:

    static void check_argument_count( const Vertica::PartitionReader &inputReader, int argcount );
    static void check_argument_count( const Vertica::SizedColumnTypes &inputTypes, int argcount );

    static void checkArgumentTimingType(const Vertica::SizedColumnTypes &inputTypes, int argIndex);
    static void checkArgumentIntType   (const Vertica::SizedColumnTypes &inputTypes, int argIndex);
    static void checkArgumentIntNumType(const Vertica::SizedColumnTypes &inputTypes, int argIndex);
    static void checkArgumentBinaryType(const Vertica::SizedColumnTypes &inputTypes, int argIndex);
    static void checkArgumentStringType(const Vertica::SizedColumnTypes &inputTypes, int argIndex);
    static std::string getColumnNameFromArg(const Vertica::SizedColumnTypes &inputTypes, int arg_index, const std::string& default_name);

};

} // namespaces VerticaUDxUtilities
