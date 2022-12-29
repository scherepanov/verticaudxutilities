#pragma once
#include "Vertica.h"

namespace VerticaUDxUtilities {

typedef std::vector<std::string> columns_t;

class Csv : public Vertica::TransformFunction {
  public:
    Csv();

  public:
    void setup(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes) override;
    void processPartition(Vertica::ServerInterface &srvInterface, Vertica::PartitionReader &inputReader, Vertica::PartitionWriter &outputWriter) override;

  private:
    bool debug;
    long column_count;
    char column_delimiter;
    char row_separator;
    long max_row_length;
    long skip_rows;
    bool filename_column;
    bool path_column;
    bool empty_as_null;
};

class CsvFactory : public Vertica::TransformFunctionFactory {
  public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
    void getParameterType(Vertica::ServerInterface &srvInterface, Vertica::SizedColumnTypes &parameterTypes) override;
    Vertica::TransformFunction *createTransformFunction(Vertica::ServerInterface &srvInterface) override;

  public:
    static void parse_parameters(Vertica::ServerInterface &srvInterface, bool& debug, char& column_delimiter, char& row_separator, long& max_row_length,
      long& skip_rows, bool& filename_column, bool& path_column, bool& empty_as_null);
    static void parseColumns(Vertica::ServerInterface &srvInterface, bool debug, char column_delimiter, char row_separator, long max_row_length, columns_t& columns);


};

} // namespaces VerticaUDxUtilities
