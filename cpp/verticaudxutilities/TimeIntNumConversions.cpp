#include "TimeIntNumConversions.h"
#include "UDxUtilities.h"

namespace VerticaUDxUtilities {

static const uint64_t SEC_PER_DAY = 24LL * 60LL * 60LL;
static const uint64_t USEC_PER_DAY = SEC_PER_DAY * 1000000LL;
static const uint64_t VERTICA_EPOCH_US = (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USEC_PER_DAY;
static const uint64_t VERTICA_EPOCH_OFFSET_DAYS = POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE;

ToIntFactory::ToIntFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void ToIntFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addAny();
  returnType.addInt();
}

void ToIntFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 1);
  UDxUtilities::checkArgumentTimingType(inputTypes, 0);
  outputTypes.addInt(UDxUtilities::getColumnNameFromArg(inputTypes, 0, "to_int"));
}

Vertica::ScalarFunction * ToIntFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ToInt);
}

void ToIntFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void ToInt::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      res_writer.setInt(arg_reader.getIntRef(0));
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
      vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(ToIntFactory);

ToNumericFactory::ToNumericFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void ToNumericFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addAny();
  returnType.addNumeric();
}

void ToNumericFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 1);
  UDxUtilities::checkArgumentTimingType(inputTypes, 0);
  int scale = 6;
  if(inputTypes.getColumnType(0).isDate())
    scale = 0;
  outputTypes.addNumeric(18, scale, UDxUtilities::getColumnNameFromArg(inputTypes, 0, "to_numeric"));
}

Vertica::ScalarFunction * ToNumericFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ToNumeric);
}

void ToNumericFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void ToNumeric::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    int scale = 6;
    if (arg_reader.getTypeMetaData().getColumnType(0).isDate())
      scale = 0;
    do {
      Vertica::vint tm = arg_reader.getIntRef(0);
      Vertica::VNumeric vnum(reinterpret_cast<Vertica::uint64*>(&tm), 18, scale);
      res_writer.getNumericRef().copy(&vnum);
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
      vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(ToNumericFactory);

ToDateFactory::ToDateFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void ToDateFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addInt();
  returnType.addDate();
}

void ToDateFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 1);
  outputTypes.addDate(UDxUtilities::getColumnNameFromArg(inputTypes, 0, "to_date"));
}

Vertica::ScalarFunction * ToDateFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ToDate);
}

void ToDateFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void ToDate::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      res_writer.setDate (arg_reader.getIntRef(0));
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
      vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(ToDateFactory);


ToTimeFactory::ToTimeFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void ToTimeFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addInt();
  returnType.addTime();
}

void ToTimeFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 1);
  outputTypes.addTime(6, UDxUtilities::getColumnNameFromArg(inputTypes, 0, "to_time"));
}

Vertica::ScalarFunction * ToTimeFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ToTime);
}

void ToTimeFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void ToTime::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      res_writer.setTime(arg_reader.getIntRef(0));
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
      vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(ToTimeFactory);


ToTimeTZFactory::ToTimeTZFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void ToTimeTZFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addInt();
  returnType.addTime();
}

void ToTimeTZFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 1);
  outputTypes.addTimeTz(6, UDxUtilities::getColumnNameFromArg(inputTypes, 0, "to_timetz"));
}

Vertica::ScalarFunction * ToTimeTZFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ToTimeTZ);
}

void ToTimeTZFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void ToTimeTZ::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      res_writer.setTimeTz(arg_reader.getIntRef(0));
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
    vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(ToTimeTZFactory);


ToTimestampFactory::ToTimestampFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void ToTimestampFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addInt();
  returnType.addTimestamp();
}

void ToTimestampFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 1);
  outputTypes.addTimestamp(6, UDxUtilities::getColumnNameFromArg(inputTypes, 0, "to_timestamp"));
}

Vertica::ScalarFunction * ToTimestampFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ToTimestamp);
}

void ToTimestampFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void ToTimestamp::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      res_writer.setTimestamp(arg_reader.getIntRef(0));
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
      vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(ToTimestampFactory);

ToTimestampTZFactory::ToTimestampTZFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void ToTimestampTZFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addInt();
  returnType.addTimestampTz();
}

void ToTimestampTZFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 1);
  outputTypes.addTimestampTz(6, UDxUtilities::getColumnNameFromArg(inputTypes, 0, "to_timestamptz"));
}

Vertica::ScalarFunction * ToTimestampTZFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ToTimestampTZ);
}

void ToTimestampTZFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void ToTimestampTZ::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      res_writer.setTimestampTz(arg_reader.getIntRef(0));
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
      vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(ToTimestampTZFactory);

UnixNanosToTimestampFactory::UnixNanosToTimestampFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void UnixNanosToTimestampFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addInt();
  returnType.addTimestamp();
}

void UnixNanosToTimestampFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 1);
  outputTypes.addTimestamp(6, UDxUtilities::getColumnNameFromArg(inputTypes, 0, "unix_nanos_to_timestamp"));
}

Vertica::ScalarFunction * UnixNanosToTimestampFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, UnixNanosToTimestamp);
}

void UnixNanosToTimestampFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void UnixNanosToTimestamp::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      res_writer.setTimestamp(arg_reader.getIntRef(0)/1000LL - VERTICA_EPOCH_US );
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
      vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(UnixNanosToTimestampFactory);

UnixMicrosToTimestampFactory::UnixMicrosToTimestampFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void UnixMicrosToTimestampFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addInt();
  returnType.addTimestamp();
}

void UnixMicrosToTimestampFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 1);
  outputTypes.addTimestamp(6, UDxUtilities::getColumnNameFromArg(inputTypes, 0, "unix_micros_to_timestamp"));
}

Vertica::ScalarFunction * UnixMicrosToTimestampFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, UnixMicrosToTimestamp);
}

void UnixMicrosToTimestampFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void UnixMicrosToTimestamp::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      res_writer.setTimestamp(arg_reader.getIntRef(0) - VERTICA_EPOCH_US );
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
      vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(UnixMicrosToTimestampFactory);

UnixNanosToTimestampTzFactory::UnixNanosToTimestampTzFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void UnixNanosToTimestampTzFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addInt();
  returnType.addTimestampTz();
}

void UnixNanosToTimestampTzFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 1);
  outputTypes.addTimestampTz(6, UDxUtilities::getColumnNameFromArg(inputTypes, 0, "unix_nanos_to_timestamp_tz"));
}

Vertica::ScalarFunction * UnixNanosToTimestampTzFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, UnixNanosToTimestampTz);
}

void UnixNanosToTimestampTzFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void UnixNanosToTimestampTz::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      res_writer.setTimestampTz(arg_reader.getIntRef(0)/1000LL - VERTICA_EPOCH_US );
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
      vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(UnixNanosToTimestampTzFactory);

UnixMicrosToTimestampTzFactory::UnixMicrosToTimestampTzFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void UnixMicrosToTimestampTzFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addInt();
  returnType.addTimestampTz();
}

void UnixMicrosToTimestampTzFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 1);
  outputTypes.addTimestampTz(6, UDxUtilities::getColumnNameFromArg(inputTypes, 0, "unix_micros_to_timestamp_tz"));
}

Vertica::ScalarFunction * UnixMicrosToTimestampTzFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, UnixMicrosToTimestampTz);
}

void UnixMicrosToTimestampTzFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void UnixMicrosToTimestampTz::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      res_writer.setTimestampTz(arg_reader.getIntRef(0) - VERTICA_EPOCH_US );
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
      vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(UnixMicrosToTimestampTzFactory);

MidnightNanosFactory::MidnightNanosFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void MidnightNanosFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addAny();
  returnType.addInt();
}

void MidnightNanosFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 2);
  std::vector<size_t> arg_cols;
  inputTypes.getArgumentColumns(arg_cols);

  Vertica::VerticaType ts_type = inputTypes.getColumnType(arg_cols[0]);
  Vertica::VerticaType ns_type = inputTypes.getColumnType(arg_cols[1]);
  if (!ts_type.isTimestamp() && !ts_type.isTimestampTz() && ts_type.isTime() )
    vt_report_error(1, "MidnightNanos expects first argument of type Time, Timestamp or TimestampTZ, get %s", ts_type.getTypeStr());
  if(!ns_type.isInt())
    vt_report_error(1, "MidnightNanos expects second argument of type Int, get %s", ns_type.getTypeStr());

  outputTypes.addTimestamp(6, UDxUtilities::getColumnNameFromArg(inputTypes, 0, "to_timestamp"));
}

Vertica::ScalarFunction * MidnightNanosFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, MidnightNanos);
}

void MidnightNanosFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void MidnightNanos::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      res_writer.setInt((arg_reader.getIntRef(0) %86400000000) * 1000 + arg_reader.getIntRef(1));
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
      vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(MidnightNanosFactory);

UnixMicrosToDateFactory::UnixMicrosToDateFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void UnixMicrosToDateFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addInt();
  returnType.addDate();
}

void UnixMicrosToDateFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 1);
  outputTypes.addDate(UDxUtilities::getColumnNameFromArg(inputTypes, 0, "unix_micros_to_date"));
}

Vertica::ScalarFunction * UnixMicrosToDateFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, UnixMicrosToDate);
}

void UnixMicrosToDateFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void UnixMicrosToDate::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      res_writer.setDate((arg_reader.getIntRef(0) - VERTICA_EPOCH_US) / USEC_PER_DAY );
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
      vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(UnixMicrosToDateFactory);

UnixDaysToDateFactory::UnixDaysToDateFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void UnixDaysToDateFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addInt();
  returnType.addDate();
}

void UnixDaysToDateFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 1);
  outputTypes.addDate(UDxUtilities::getColumnNameFromArg(inputTypes, 0, "unix_days_to_date"));
}

Vertica::ScalarFunction * UnixDaysToDateFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, UnixDaysToDate);
}

void UnixDaysToDateFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void UnixDaysToDate::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      res_writer.setDate((arg_reader.getIntRef(0) - VERTICA_EPOCH_OFFSET_DAYS) );
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
    vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(UnixDaysToDateFactory);

MicrosSinceEpochFactory::MicrosSinceEpochFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void MicrosSinceEpochFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addDate();
  argTypes.addTime();
  returnType.addInt();
}

void MicrosSinceEpochFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 2);
  outputTypes.addInt("micros_since_epoch");
}

Vertica::ScalarFunction * MicrosSinceEpochFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, MicrosSinceEpoch);
}

void MicrosSinceEpochFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void MicrosSinceEpoch::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      res_writer.setInt((arg_reader.getDateRef(0) + VERTICA_EPOCH_OFFSET_DAYS) * USEC_PER_DAY + arg_reader.getTimeRef(1) );
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
    vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(MicrosSinceEpochFactory);

NanosSinceEpochFactory::NanosSinceEpochFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void NanosSinceEpochFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addDate();
  argTypes.addTime();
  argTypes.addInt();
  returnType.addInt();
}

void NanosSinceEpochFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 3);
  outputTypes.addInt("micros_since_epoch");
}

Vertica::ScalarFunction * NanosSinceEpochFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, NanosSinceEpoch);
}

void NanosSinceEpochFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void NanosSinceEpoch::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      res_writer.setInt(((arg_reader.getDateRef(0) + VERTICA_EPOCH_OFFSET_DAYS) * USEC_PER_DAY + arg_reader.getTimeRef(1)) * 1000LL + arg_reader.getIntRef(2));
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
    vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(NanosSinceEpochFactory);

} // namespaces VerticaUDxUtilities
