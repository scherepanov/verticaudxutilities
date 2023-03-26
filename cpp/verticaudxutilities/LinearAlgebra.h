#pragma once
#include "Vertica.h"

namespace VerticaUDxUtilities {

class ArrayLinearInterpolateFactory : public Vertica::ScalarFunctionFactory {
public:
  ArrayLinearInterpolateFactory();
public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
};

class ArrayLinearInterpolate : public Vertica::ScalarFunction {
public:
  void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &inputReader, Vertica::BlockWriter &outputWriter) override;
};


class ArrayMakeFactory : public Vertica::ScalarFunctionFactory {
public:
  ArrayMakeFactory();
public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getParameterType(Vertica::ServerInterface &srvInterface, Vertica::SizedColumnTypes &parameterTypes) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
};

class ArrayMake : public Vertica::ScalarFunction {
public:
  void setup(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes) override;
  void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &inputReader, Vertica::BlockWriter &outputWriter) override;
private:
  int elements;
};


class ArrayNegFactory : public Vertica::ScalarFunctionFactory {
public:
  ArrayNegFactory();
public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
};

class ArrayNeg : public Vertica::ScalarFunction {
public:
  void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &inputReader, Vertica::BlockWriter &outputWriter) override;
};


class ArrayBase : public Vertica::ScalarFunction {
public:
  void setup(Vertica::ServerInterface& srvInterface, const Vertica::SizedColumnTypes &argTypes) override;
  void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &inputReader, Vertica::BlockWriter &outputWriter) override;
protected:
  virtual void calculate(Vertica::Array::ArrayReader& inputArrayReader, double scalar, Vertica::Array::ArrayWriter& outputArrayWriter) = 0;
  virtual void calculate(Vertica::Array::ArrayReader& inputArrayReader, Vertica::Array::ArrayReader& inputArrayReader1, Vertica::Array::ArrayWriter& outputArrayWriter) = 0;
private:
  bool scalar_present;
  int scalar_ind;
  int array_ind;
};

class ArrayAdd : public ArrayBase {
protected:
  void calculate(Vertica::Array::ArrayReader& inputArrayReader, double scalar, Vertica::Array::ArrayWriter& outputArrayWriter) override;
  void calculate(Vertica::Array::ArrayReader& inputArrayReader, Vertica::Array::ArrayReader& inputArrayReader1, Vertica::Array::ArrayWriter& outputArrayWriter) override;
};

class ArraySub : public ArrayBase {
protected:
  void calculate(Vertica::Array::ArrayReader& inputArrayReader, double scalar, Vertica::Array::ArrayWriter& outputArrayWriter) override;
  void calculate(Vertica::Array::ArrayReader& inputArrayReader, Vertica::Array::ArrayReader& inputArrayReader1, Vertica::Array::ArrayWriter& outputArrayWriter) override;
};

class ArrayMul : public ArrayBase {
protected:
  void calculate(Vertica::Array::ArrayReader& inputArrayReader, double scalar, Vertica::Array::ArrayWriter& outputArrayWriter) override;
  void calculate(Vertica::Array::ArrayReader& inputArrayReader, Vertica::Array::ArrayReader& inputArrayReader1, Vertica::Array::ArrayWriter& outputArrayWriter) override;
};

class ArrayDiv : public ArrayBase {
protected:
  void calculate(Vertica::Array::ArrayReader& inputArrayReader, double scalar, Vertica::Array::ArrayWriter& outputArrayWriter) override;
  void calculate(Vertica::Array::ArrayReader& inputArrayReader, Vertica::Array::ArrayReader& inputArrayReader1, Vertica::Array::ArrayWriter& outputArrayWriter) override;
};

class ArrayBaseFactory : public Vertica::ScalarFunctionFactory {
public:
  ArrayBaseFactory();
public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class ArrayBase0Factory : public ArrayBaseFactory {
public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
};

class ArrayBase1Factory : public ArrayBaseFactory {
public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
};


class ArrayAddFactory : public ArrayBaseFactory {
public:
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
};

class ArraySubFactory : public ArrayBaseFactory {
public:
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
};

class ArrayMulFactory : public ArrayBaseFactory {
public:
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
};

class ArrayDivFactory : public ArrayBaseFactory {
public:
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
};


class ArrayAdd0Factory : public ArrayBase0Factory {
public:
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
};

class ArraySub0Factory : public ArrayBase0Factory {
public:
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
};

class ArrayMul0Factory : public ArrayBase0Factory {
public:
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
};

class ArrayDiv0Factory : public ArrayBase0Factory {
public:
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
};


class ArrayAdd1Factory : public ArrayBase1Factory {
public:
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
};

class ArraySub1Factory : public ArrayBase1Factory {
public:
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
};

class ArrayMul1Factory : public ArrayBase1Factory {
public:
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
};

class ArrayDiv1Factory : public ArrayBase1Factory {
public:
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
};

} // namespace VerticaUDxUtilities

