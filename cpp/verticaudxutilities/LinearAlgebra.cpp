#include "LinearAlgebra.h"
#include "Arrays/Accessors.h"

namespace VerticaUDxUtilities {
using namespace Basics;

ArrayLinearInterpolateFactory::ArrayLinearInterpolateFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void ArrayLinearInterpolateFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addArrayType(Float8OID);
  argTypes.addArrayType(Float8OID);
  argTypes.addFloat();
  returnType.addFloat();
}

void ArrayLinearInterpolateFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  outputTypes.addFloat("Y");
}

void ArrayLinearInterpolateFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

Vertica::ScalarFunction * ArrayLinearInterpolateFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ArrayLinearInterpolate);
}

void ArrayLinearInterpolate::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &inputReader, Vertica::BlockWriter &outputWriter) {
  do {
    if (inputReader.isNull(0) or inputReader.isNull(1) or inputReader.isNull(2)) {
      outputWriter.setNull();
      outputWriter.next();
      continue;
    }
    auto x_array_reader = inputReader.getArrayRef(0);
    auto y_array_reader = inputReader.getArrayRef(1);
    auto X = inputReader.getFloatRef(2);
    double x_prev = 0;
    double y_prev = 0;
    double x_el = 0;
    double y_el = 0;
    int cnt = 0;
    bool value_set = false;
    for (; x_array_reader->hasData();) {
      cnt++;
      if(!y_array_reader->hasData()) {
        vt_report_error(101, "y array has less elements than x array");
      }
      x_el = x_array_reader->getFloatRef(0);
      y_el = y_array_reader->getFloatRef(0);
      x_array_reader->next();
      y_array_reader->next();
      if(cnt == 1) {
        if (X < x_el) {
          outputWriter.setNull();
          value_set = true;
          break;
        }
      } else {
        if(x_el <= x_prev) {
          vt_report_error(103, "Element in array X %f is less or equal than previous %f", x_el, x_prev);
        } else if (x_el >= X) {
          outputWriter.setFloat(y_prev + (y_el - y_prev) * (X - x_prev)/(x_el - x_prev));
          value_set = true;
          break;
        }
      }
      x_prev = x_el;
      y_prev = y_el;
    }
    if (!value_set and cnt < 2) {
      vt_report_error(104, "Array X need to have at least two values");
    }
    if(!value_set) {
      outputWriter.setNull();
    }
    outputWriter.next();
  } while ( inputReader.next() );
}

RegisterFactory(ArrayLinearInterpolateFactory);

ArrayMakeFactory::ArrayMakeFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void ArrayMakeFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addFloat();
  returnType.addArrayType(Float8OID);
}

void ArrayMakeFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  int elements = static_cast<int>(srvInterface.getParamReader().getIntRef("elements"));
  if(elements < 1 or elements > 10240) {
    vt_report_error(102, "Parameter elements should be between 1 and 10240");
  }
  outputTypes.addArrayType(Field(Float8OID, ""), "array", elements);
}

void ArrayMakeFactory::getParameterType(ServerInterface &srvInterface, SizedColumnTypes &parameterTypes) {
  parameterTypes.addInt("elements", Fields::Properties(true, true, false, ""));
}

void ArrayMakeFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

Vertica::ScalarFunction * ArrayMakeFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ArrayMake);
}

void ArrayMake::setup(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes) {
  elements = static_cast<int>(srvInterface.getParamReader().getIntRef("elements"));
}

void ArrayMake::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &inputReader, Vertica::BlockWriter &outputWriter) {
  do {
    if (inputReader.isNull(0)) {
      outputWriter.setNull();
      outputWriter.next();
      continue;
    }
    auto fill = inputReader.getFloatRef(0);
    auto outputArrayWriter = outputWriter.getArrayRef(0);
    for (int i = 0; i < elements; i++) {
      outputArrayWriter->setFloat(0, fill);
      outputArrayWriter->next();
    }
    outputArrayWriter.commit();
    outputWriter.next();
  } while ( inputReader.next() );
}

RegisterFactory(ArrayMakeFactory);

ArrayNegFactory::ArrayNegFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void ArrayNegFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addArrayType(Float8OID);
  returnType.addArrayType(Float8OID);
}

void ArrayNegFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  auto array_len = inputTypes.getColumnType(0).getArrayBound();
  outputTypes.addArrayType(Field(Float8OID, ""), "array", array_len);
}

void ArrayNegFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

Vertica::ScalarFunction * ArrayNegFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ArrayNeg);
}

void ArrayNeg::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &inputReader, Vertica::BlockWriter &outputWriter) {
  do {
    if (inputReader.isNull(0)) {
      outputWriter.setNull();
      outputWriter.next();
      continue;
    }
    auto inputArrayReader = inputReader.getArrayRef(0);
    auto outputArrayWriter = outputWriter.getArrayRef(0);
    for (; inputArrayReader->hasData();) {
      outputArrayWriter->setFloat(0, - inputArrayReader->getFloatRef(0));
      inputArrayReader->next();
      outputArrayWriter->next();
    }
    outputArrayWriter.commit();
    outputWriter.next();
  } while ( inputReader.next() );
}

RegisterFactory(ArrayNegFactory);


void ArrayBase::setup(Vertica::ServerInterface& srvInterface, const Vertica::SizedColumnTypes &argTypes) {
  bool float0 = argTypes.getColumnType(0).isFloat();
  bool float1 = argTypes.getColumnType(1).isFloat();
  scalar_present = float0 or float1;
  scalar_ind = float0 ? 0 : 1;
  array_ind = float0 ? 1 : 0;
}

void ArrayBase::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &inputReader, Vertica::BlockWriter &outputWriter) {
  do {
    if (inputReader.isNull(0) or inputReader.isNull(1)) {
      outputWriter.setNull();
      outputWriter.next();
      continue;
    }
    auto inputArrayReader = inputReader.getArrayRef(array_ind);
    auto outputArrayWriter = outputWriter.getArrayRef(0);
    if (scalar_present) {
      auto scalar = inputReader.getFloatRef(scalar_ind);
      for (; inputArrayReader->hasData();) {
        calculate(inputArrayReader, scalar, outputArrayWriter);
        inputArrayReader->next();
        outputArrayWriter->next();
      }
    } else {
      auto inputArrayReader1 = inputReader.getArrayRef(1);
      for (; inputArrayReader->hasData();) {
        if( not inputArrayReader1->hasData()) {
          vt_report_error(105, "Second array has less elements than first");
        }
        calculate(inputArrayReader, inputArrayReader1, outputArrayWriter);
        inputArrayReader->next();
        inputArrayReader1->next();
        outputArrayWriter->next();
      }
      if( inputArrayReader1->hasData()) {
        vt_report_error(106, "Second array has more elements than first");
      }
    }
    outputArrayWriter.commit();
    outputWriter.next();
  } while ( inputReader.next() );
}


void ArrayAdd::calculate(Vertica::Array::ArrayReader& inputArrayReader, double scalar, Vertica::Array::ArrayWriter& outputArrayWriter) {
  outputArrayWriter->setFloat(0, inputArrayReader->getFloatRef(0) + scalar);
}

void ArrayAdd::calculate(Vertica::Array::ArrayReader& inputArrayReader, Vertica::Array::ArrayReader& inputArrayReader1, Vertica::Array::ArrayWriter& outputArrayWriter) {
  outputArrayWriter->setFloat(0, inputArrayReader->getFloatRef(0) + inputArrayReader1->getFloatRef(0));
}

void ArraySub::calculate(Vertica::Array::ArrayReader& inputArrayReader, double scalar, Vertica::Array::ArrayWriter& outputArrayWriter) {
  outputArrayWriter->setFloat(0, inputArrayReader->getFloatRef(0) - scalar);
}

void ArraySub::calculate(Vertica::Array::ArrayReader& inputArrayReader, Vertica::Array::ArrayReader& inputArrayReader1, Vertica::Array::ArrayWriter& outputArrayWriter) {
  outputArrayWriter->setFloat(0, inputArrayReader->getFloatRef(0) - inputArrayReader1->getFloatRef(0));
}

void ArrayMul::calculate(Vertica::Array::ArrayReader& inputArrayReader, double scalar, Vertica::Array::ArrayWriter& outputArrayWriter) {
  outputArrayWriter->setFloat(0, inputArrayReader->getFloatRef(0) * scalar);
}

void ArrayMul::calculate(Vertica::Array::ArrayReader& inputArrayReader, Vertica::Array::ArrayReader& inputArrayReader1, Vertica::Array::ArrayWriter& outputArrayWriter) {
  outputArrayWriter->setFloat(0, inputArrayReader->getFloatRef(0) * inputArrayReader1->getFloatRef(0));
}

void ArrayDiv::calculate(Vertica::Array::ArrayReader& inputArrayReader, double scalar, Vertica::Array::ArrayWriter& outputArrayWriter) {
  if (scalar == 0) {
    outputArrayWriter->setFloat(0, Vertica::vfloat_null);
  } else {
    outputArrayWriter->setFloat(0, inputArrayReader->getFloatRef(0) / scalar);
  }
}

void ArrayDiv::calculate(Vertica::Array::ArrayReader& inputArrayReader, Vertica::Array::ArrayReader& inputArrayReader1, Vertica::Array::ArrayWriter& outputArrayWriter) {
  auto float1 = inputArrayReader1->getFloatRef(0);
  if (float1 == 0) {
    outputArrayWriter->setFloat(0, Vertica::vfloat_null);
  } else {
    outputArrayWriter->setFloat(0, inputArrayReader->getFloatRef(0) / float1);
  }
}

ArrayBaseFactory::ArrayBaseFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void ArrayBaseFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addArrayType(Float8OID);
  argTypes.addArrayType(Float8OID);
  returnType.addArrayType(Float8OID);
}

void ArrayBaseFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  int array_ind = inputTypes.getColumnType(0).isFloat() ? 1 : 0;
  auto array_len = inputTypes.getColumnType(array_ind).getArrayBound();
  outputTypes.addArrayType(Field(Float8OID, ""), "array", array_len);
}

void ArrayBaseFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 0;
}

void ArrayBase0Factory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addFloat();
  argTypes.addArrayType(Float8OID);
  returnType.addArrayType(Float8OID);
}

void ArrayBase1Factory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addArrayType(Float8OID);
  argTypes.addFloat();
  returnType.addArrayType(Float8OID);
}


Vertica::ScalarFunction * ArrayAddFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ArrayAdd);
}

RegisterFactory(ArrayAddFactory);

Vertica::ScalarFunction * ArraySubFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ArraySub);
}

RegisterFactory(ArraySubFactory);

Vertica::ScalarFunction * ArrayMulFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ArrayMul);
}

RegisterFactory(ArrayMulFactory);

Vertica::ScalarFunction * ArrayDivFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ArrayDiv);
}

RegisterFactory(ArrayDivFactory);


Vertica::ScalarFunction * ArrayAdd0Factory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ArrayAdd);
}

RegisterFactory(ArrayAdd0Factory);

Vertica::ScalarFunction * ArraySub0Factory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ArraySub);
}

RegisterFactory(ArraySub0Factory);

Vertica::ScalarFunction * ArrayMul0Factory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ArrayMul);
}

RegisterFactory(ArrayMul0Factory);

Vertica::ScalarFunction * ArrayDiv0Factory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ArrayDiv);
}

RegisterFactory(ArrayDiv0Factory);


Vertica::ScalarFunction * ArrayAdd1Factory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ArrayAdd);
}

RegisterFactory(ArrayAdd1Factory);

Vertica::ScalarFunction * ArraySub1Factory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ArraySub);
}

RegisterFactory(ArraySub1Factory);

Vertica::ScalarFunction * ArrayMul1Factory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ArrayMul);
}

RegisterFactory(ArrayMul1Factory);

Vertica::ScalarFunction * ArrayDiv1Factory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, ArrayDiv);
}

RegisterFactory(ArrayDiv1Factory);

} // namespace VerticaUDxUtilities
