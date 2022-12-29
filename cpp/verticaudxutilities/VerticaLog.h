#pragma once
#include "Vertica.h"

namespace VerticaUDxUtilities {

class VerticaLogFactory : public Vertica::TransformFunctionFactory {
public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  void getParameterType(Vertica::ServerInterface &srvInterface, Vertica::SizedColumnTypes &parameterTypes) override;
  Vertica::TransformFunction *createTransformFunction(Vertica::ServerInterface &srvInterface) override;
};

class VerticaLog : public Vertica::TransformFunction {

public:
  VerticaLog();

public:
  void setup(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes & 	argTypes ) override;
  void processPartition(Vertica::ServerInterface &srvInterface, Vertica::PartitionReader &inputReader, Vertica::PartitionWriter &outputWriter) override;

private:
  std::string getParamStr(const Vertica::ParamReader& params, const std::string& param_name);
  void getParamStr(const Vertica::ParamReader& params, const std::string& param_name, std::string& out_var, bool& out_flag);
  void getNextToken(const char* str_ptr, size_t str_len, char delimiter, size_t& from_ind, size_t& token_len);
  void prepareProcess(Vertica::ServerInterface &srvInterface);

protected:
  std::string node;
  std::string date;
  std::string catalog_dir;
  std::string log_file;
  bool log_file_exist;
  bool node_match;
  ssize_t last_mb;
  bool udx;
  std::string log_date;
  std::string time_start;
  std::string time_end;
  std::string txn;
  std::string contains_param;
  std::string contains2_param;
  std::string exclude_param;
  bool log_date_flag;
  bool time_started;
  bool time_start_flag;
  bool time_end_flag;
  bool txn_flag;
  bool contains_flag;
  bool contains2_flag;
  bool exclude_flag;
  bool debug;
  std::ifstream inp;
  std::string current_node_name;
  std::string line;
};

class VerticaLogSizeFactory : public Vertica::TransformFunctionFactory {
public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  void getParameterType(Vertica::ServerInterface &srvInterface, Vertica::SizedColumnTypes &parameterTypes) override;
  Vertica::TransformFunction *createTransformFunction(Vertica::ServerInterface &srvInterface) override;
};

class VerticaLogSize: public VerticaLog {
  void processPartition(Vertica::ServerInterface &srvInterface, Vertica::PartitionReader &inputReader, Vertica::PartitionWriter &outputWriter) override;
};

} // namespaces VerticaUDxUtilities
