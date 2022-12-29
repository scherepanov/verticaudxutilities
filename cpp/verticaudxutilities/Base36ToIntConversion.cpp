#include "Base36ToIntConversion.h"
#include "UDxUtilities.h"

namespace VerticaUDxUtilities {

void convertBase36ToInt(Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  if(arg_reader.isNull(0)) {
    res_writer.setNull();
  } else {
    std::string b_str = arg_reader.getStringRef(0).str();
    if(b_str.length() == 0 || b_str.length() > 13) {
      res_writer.setNull();
      return;
    }
    uint64_t res = 0;
    for(uint i = 0; i< b_str.length(); i++) {
      char c = b_str[i];
      if(i == 0 && b_str.length() == 13 && c != '0' && c != '1' && c != '2') {
        res_writer.setNull();
        return;
      }
      if(c >= '0' && c <= '9') {
        res = res*36 + (int)c - '0';
      } else if (c >= 'a' && c <= 'z'){
        res = res*36 + (int)c - 'a' + 10;
      } else if (c >= 'A' && c <= 'Z'){
        res = res*36 + (int)c - 'A' + 10;
      } else {
        res_writer.setNull();
        return;
      }
    }
    res_writer.setInt(static_cast<Vertica::vint>(res));
  }
}

void convertIntIoBase36(Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  if(arg_reader.isNull(0)) {
    res_writer.setNull();
  } else {
    static char base36[37] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    uint64_t inp = arg_reader.getIntRef(0);
    char buffer[14];
    unsigned int offset = sizeof(buffer);
    offset--;
    do {
      offset--;
      buffer[offset] = base36[inp %36];
    } while (inp /= 36);
    res_writer.getStringRef().copy(buffer + offset, 13 - offset);
  }
}

Base36ToIntFactory::Base36ToIntFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void Base36ToIntFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addVarchar();
  returnType.addInt();
}

void Base36ToIntFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 1);
  UDxUtilities::checkArgumentStringType(inputTypes, 0);
  outputTypes.addInt(UDxUtilities::getColumnNameFromArg(inputTypes, 0, "base36_to_int"));
}

Vertica::ScalarFunction * Base36ToIntFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, Base36ToInt);
}

void Base36ToIntFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void Base36ToInt::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      convertBase36ToInt(arg_reader, res_writer);
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
      vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(Base36ToIntFactory);

IntToBase36Factory::IntToBase36Factory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void IntToBase36Factory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addInt();
  returnType.addVarchar();
}

void IntToBase36Factory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 1);
  UDxUtilities::checkArgumentIntType(inputTypes, 0);
  outputTypes.addVarchar(14, UDxUtilities::getColumnNameFromArg(inputTypes, 0, "int_to_base36"));
}

Vertica::ScalarFunction * IntToBase36Factory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, IntToBase36);
}

void IntToBase36Factory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void IntToBase36::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      convertIntIoBase36(arg_reader, res_writer);
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
      vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(IntToBase36Factory);

} // namespaces VerticaUDxUtilities
