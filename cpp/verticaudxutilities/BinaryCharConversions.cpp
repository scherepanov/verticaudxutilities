#include "BinaryCharConversions.h"
#include "UDxUtilities.h"

namespace VerticaUDxUtilities {

BinaryToCharFactory::BinaryToCharFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void BinaryToCharFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addAny();
  returnType.addVarchar();
}

void BinaryToCharFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 1);
  UDxUtilities::checkArgumentBinaryType(inputTypes, 0);
  outputTypes.addVarchar(inputTypes.getColumnType(0).getStringLength(), UDxUtilities::getColumnNameFromArg(inputTypes, 0, "bin_to_char"));
}

Vertica::ScalarFunction * BinaryToCharFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, BinaryToChar);
}

void BinaryToCharFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void BinaryToChar::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      res_writer.getStringRef().copy(arg_reader.getStringRef(0));
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
      vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(BinaryToCharFactory);

CharToBinaryFactory::CharToBinaryFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void CharToBinaryFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addAny();
  returnType.addVarbinary();
}

void CharToBinaryFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 1);
  UDxUtilities::checkArgumentStringType(inputTypes, 0);
  outputTypes.addVarbinary(inputTypes.getColumnType(0).getStringLength(), UDxUtilities::getColumnNameFromArg(inputTypes, 0, "char_to_bin"));
}

Vertica::ScalarFunction * CharToBinaryFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, CharToBinary);
}

void CharToBinaryFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void CharToBinary::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      res_writer.getStringRef().copy(arg_reader.getStringRef(0));
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
      vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(CharToBinaryFactory);

} // namespaces VerticaUDxUtilities
