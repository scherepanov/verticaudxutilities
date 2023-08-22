#pragma once
#include <Vertica.h>

namespace VerticaUDxUtilities {

class ToInt : public Vertica::ScalarFunction {
  public:
    ToInt();
  public:
    void setup(	Vertica::ServerInterface& 	srvInterface, const Vertica::SizedColumnTypes&	argTypes ) override;
    void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
  private:
    bool time_tz;
};

class ToIntFactory : public Vertica::ScalarFunctionFactory {
  public:
    ToIntFactory();

  public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
    Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
    void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class ToNumeric : public Vertica::ScalarFunction {
  public:
    ToNumeric();
  public:
    void setup(	Vertica::ServerInterface& 	srvInterface, const Vertica::SizedColumnTypes&	argTypes ) override;
    void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
  private:
    bool time_tz;
};

class ToNumericFactory : public Vertica::ScalarFunctionFactory {
  public:
    ToNumericFactory();

  public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
    Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
    void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class ToDate : public Vertica::ScalarFunction {
  public:
    void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class ToDateFactory : public Vertica::ScalarFunctionFactory {
  public:
    ToDateFactory();

  public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
    Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
    void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class ToTime : public Vertica::ScalarFunction {
  public:
    void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class ToTimeFactory : public Vertica::ScalarFunctionFactory {
  public:
    ToTimeFactory();

  public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
    Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
    void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class ToTimeTZ : public Vertica::ScalarFunction {
public:
    void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class ToTimeTZFactory : public Vertica::ScalarFunctionFactory {
public:
    ToTimeTZFactory();

public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
    Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
    void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class ToTimestamp : public Vertica::ScalarFunction {
  public:
    void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class ToTimestampFactory : public Vertica::ScalarFunctionFactory {
  public:
    ToTimestampFactory();

  public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
    Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
    void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class ToTimestampTZ : public Vertica::ScalarFunction {
  public:
    void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class ToTimestampTZFactory : public Vertica::ScalarFunctionFactory {
  public:
    ToTimestampTZFactory();

  public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
    Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
    void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class UnixDaysToDate : public Vertica::ScalarFunction {
public:
  void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class UnixDaysToDateFactory : public Vertica::ScalarFunctionFactory {
public:
  UnixDaysToDateFactory();

public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
  void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class DateToUnixDays : public Vertica::ScalarFunction {
public:
  void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class DateToUnixDaysFactory : public Vertica::ScalarFunctionFactory {
public:
  DateToUnixDaysFactory();

public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
  void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class UnixMicrosToTimestamp : public Vertica::ScalarFunction {
public:
  void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class UnixMicrosToTimestampFactory : public Vertica::ScalarFunctionFactory {
public:
  UnixMicrosToTimestampFactory();

public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
  void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class TimestampToUnixMicros : public Vertica::ScalarFunction {
public:
  void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class TimestampToUnixMicrosFactory : public Vertica::ScalarFunctionFactory {
public:
  TimestampToUnixMicrosFactory();

public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
  void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class MidnightMicrosToTime : public Vertica::ScalarFunction {
public:
  void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class MidnightMicrosToTimeFactory : public Vertica::ScalarFunctionFactory {
public:
  MidnightMicrosToTimeFactory();

public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
  void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class TimeToMidnightMicros : public Vertica::ScalarFunction {
public:
  void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class TimeToMidnightMicrosFactory : public Vertica::ScalarFunctionFactory {
public:
  TimeToMidnightMicrosFactory();

public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
  void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class MidnightMicrosToTimeTz : public Vertica::ScalarFunction {
public:
  void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class MidnightMicrosToTimeTzFactory : public Vertica::ScalarFunctionFactory {
public:
  MidnightMicrosToTimeTzFactory();

public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
  void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class TimeTzToMidnightMicros : public Vertica::ScalarFunction {
public:
  void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class TimeTzToMidnightMicrosFactory : public Vertica::ScalarFunctionFactory {
public:
  TimeTzToMidnightMicrosFactory();

public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
  void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};


class UnixMicrosToDate : public Vertica::ScalarFunction {
public:
  void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class UnixMicrosToDateFactory : public Vertica::ScalarFunctionFactory {
public:
  UnixMicrosToDateFactory();

public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
  void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class UnixNanosToTimestamp : public Vertica::ScalarFunction {
  public:
    void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class UnixNanosToTimestampFactory : public Vertica::ScalarFunctionFactory {
  public:
    UnixNanosToTimestampFactory();

  public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
    Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
    void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class UnixNanosToTimestampTz : public Vertica::ScalarFunction {
  public:
    void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class UnixNanosToTimestampTzFactory : public Vertica::ScalarFunctionFactory {
  public:
    UnixNanosToTimestampTzFactory();

  public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
    Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
    void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class UnixMicrosToTimestampTz : public Vertica::ScalarFunction {
  public:
    void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class UnixMicrosToTimestampTzFactory : public Vertica::ScalarFunctionFactory {
  public:
    UnixMicrosToTimestampTzFactory();

  public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
    Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
    void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class MicrosSinceEpoch : public Vertica::ScalarFunction {
public:
  void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class MicrosSinceEpochFactory : public Vertica::ScalarFunctionFactory {
public:
  MicrosSinceEpochFactory();

public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
  void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class NanosSinceEpoch : public Vertica::ScalarFunction {
public:
  void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class NanosSinceEpochFactory : public Vertica::ScalarFunctionFactory {
public:
  NanosSinceEpochFactory();

public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
  void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class MidnightNanos : public Vertica::ScalarFunction {
public:
  void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class MidnightNanosFactory : public Vertica::ScalarFunctionFactory {
public:
  MidnightNanosFactory();

public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
  void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

} // namespaces VerticaUDxUtilities
