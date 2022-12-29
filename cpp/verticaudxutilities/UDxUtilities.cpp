#include "UDxUtilities.h"

namespace VerticaUDxUtilities {

void UDxUtilities::check_argument_count(const Vertica::PartitionReader &inputReader, int argcount ) {
  if (inputReader.getNumCols() != static_cast<size_t>(argcount))
    vt_report_error(0, "Function only accepts %zu argument, but %zu provided", argcount, inputReader.getNumCols());
}

void UDxUtilities::check_argument_count(const Vertica::SizedColumnTypes &inputTypes, int argcount ) {
  if (inputTypes.getColumnCount() != static_cast<size_t>(argcount))
    vt_report_error(0, "Function only accepts %zu argument, but %zu provided", argcount, inputTypes.getColumnCount());
}

void UDxUtilities::checkArgumentTimingType(const Vertica::SizedColumnTypes &inputTypes, int arg_index) {
  const Vertica::VerticaType& tp = inputTypes.getColumnType(arg_index);
  if(!tp.isDate() && !tp.isInterval() && !tp.isIntervalYM() && !tp.isTime() && !tp.isTimestamp() && !tp.isTimestampTz() && !tp.isTimeTz() )
    vt_report_error(1, "Input type should be Date, Time, TimeTZ, Timestamp, TimestampTZ, Interval or IntervalYM, received %s",
      tp.getPrettyPrintStr().c_str());
}

void UDxUtilities::checkArgumentIntType(const Vertica::SizedColumnTypes &inputTypes, int arg_index) {
  const Vertica::VerticaType& tp = inputTypes.getColumnType(arg_index);
  if(!tp.isInt())
    vt_report_error(2, "Input type should be Int");
}

void UDxUtilities::checkArgumentIntNumType(const Vertica::SizedColumnTypes &inputTypes, int arg_index) {
  const Vertica::VerticaType& tp = inputTypes.getColumnType(arg_index);
  if(!tp.isInt() && !tp.isNumeric())
    vt_report_error(2, "Input type should be Int or Numeric");
}

void UDxUtilities::checkArgumentBinaryType(const Vertica::SizedColumnTypes &inputTypes, int argIndex) {
  const Vertica::VerticaType& tp = inputTypes.getColumnType(argIndex);
  if(!tp.isBinary() && !tp.isVarbinary() && !tp.isLongVarbinary())
    vt_report_error(3, "Input type should be Binary, Varbinary or LongVarbinary");
}

void UDxUtilities::checkArgumentStringType(const Vertica::SizedColumnTypes &inputTypes, int argIndex) {
  const Vertica::VerticaType& tp = inputTypes.getColumnType(argIndex);
  if(!tp.isChar() && !tp.isVarchar() && !tp.isLongVarchar())
    vt_report_error(3, "Input type should be Char, Varchar or LongVarchar");
}

std::string UDxUtilities::getColumnNameFromArg(const Vertica::SizedColumnTypes &inputTypes, int arg_index, const std::string& default_name) {
  std::string name = inputTypes.getColumnName(arg_index);
  if(name.length() == 0)
    return default_name;
  return name;
}

RegisterLibrary("Sergey Cherepanov",
                "11/12/2022",
                "4.0",
                "11.1.1",
                "",
                "Vertica UDx Utilities",
                "",
                "");

} // namespaces VerticaUDxUtilities
