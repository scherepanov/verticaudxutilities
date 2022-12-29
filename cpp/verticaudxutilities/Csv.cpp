#include "Csv.h"
#include "UDxUtilities.h"

#include <fstream>

namespace VerticaUDxUtilities {

Csv::Csv():
  debug(false),
  column_count(32),
  column_delimiter(','),
  row_separator('\n'),
  max_row_length(4096),
  skip_rows(0),
  filename_column(false),
  path_column(false),
  empty_as_null(false)
{}

void CsvFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addVarchar();  // file name
  returnType.addAny();
}

void CsvFactory::getParameterType(Vertica::ServerInterface &srvInterface, Vertica::SizedColumnTypes &parameterTypes) {
  parameterTypes.addVarchar(32, "debug");
  parameterTypes.addVarchar(4096, "column_names");
  parameterTypes.addVarchar(256, "file_with_header");
  parameterTypes.addInt("column_count");
  parameterTypes.addVarchar(1, "column_delimiter");
  parameterTypes.addVarchar(1, "row_separator");
  parameterTypes.addInt("max_row_length");
  parameterTypes.addInt("skip_rows");
  parameterTypes.addVarchar(32, "filename_column");
  parameterTypes.addVarchar(32, "path_column");
  parameterTypes.addVarchar(32, "empty_as_null");
}

void CsvFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 1);
  bool debug = false;
  char column_delimiter = ',';
  char row_separator = '\n';
  long max_row_length = 4096;
  long skip_rows = 0;
  bool filename_column = false;
  bool path_column = false;
  bool empty_as_null = false;
  columns_t columns;

  parse_parameters(srvInterface, debug, column_delimiter, row_separator, max_row_length, skip_rows, filename_column, path_column, empty_as_null);

  parseColumns(srvInterface, debug, column_delimiter, row_separator, max_row_length, columns);

  for(const auto& column: columns)
    outputTypes.addVarchar(64, column);
  if(filename_column)
    outputTypes.addVarchar(64, "filename");
  if(path_column)
    outputTypes.addVarchar(64, "path");

  if(debug) {
    for (const auto& c : columns) {
      srvInterface.log("Columns %s", c.c_str());
    }
  }
}

Vertica::TransformFunction * CsvFactory::createTransformFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, Csv);
}

void CsvFactory::parse_parameters(Vertica::ServerInterface &srvInterface, bool& debug, char& column_delimiter, char& row_separator,
    long& max_row_length, long& skip_rows, bool& filename_column, bool& path_column, bool& empty_as_null) {

  std::string column_names;
  std::string file_with_header;

  debug = srvInterface.getParamReader().containsParameter("debug");

  if(srvInterface.getParamReader().containsParameter("column_delimiter")) {
    if(srvInterface.getParamReader().getStringRef("column_delimiter").str().length() != 1)
      vt_report_error(4, "Bad column_delimiter %s", srvInterface.getParamReader().getStringRef("column_delimiter").str().c_str());
    column_delimiter = srvInterface.getParamReader().getStringRef("column_delimiter").str()[0];
  }

  if(srvInterface.getParamReader().containsParameter("row_separator")) {
    if(srvInterface.getParamReader().getStringRef("row_separator").str().length() != 1)
      vt_report_error(5, "Bad row_separator %s", srvInterface.getParamReader().getStringRef("row_separator").str().c_str());
    row_separator = srvInterface.getParamReader().getStringRef("row_separator").str()[0];
  }

  if(srvInterface.getParamReader().containsParameter("max_row_length")) {
    max_row_length = srvInterface.getParamReader().getIntRef("max_row_length");
    if(max_row_length < 1 || max_row_length > 4096)
      vt_report_error(6, "Bad parameter max_row_length %i", max_row_length);
  }

  if(srvInterface.getParamReader().containsParameter("skip_rows")) {
    skip_rows = srvInterface.getParamReader().getIntRef("skip_rows");
    if(skip_rows < 0 )
      vt_report_error(7, "Bad parameter skip_rows %i", skip_rows);
  }

  filename_column = srvInterface.getParamReader().containsParameter("filename_column");
  path_column = srvInterface.getParamReader().containsParameter("path_column");
  empty_as_null = srvInterface.getParamReader().containsParameter("empty_as_null");

  if(debug)
    srvInterface.log("Parameters column_delimiter %c row_separator %c max_row_length %li skip_rows %li filename_column %s path_column %s empty_as_null %s",
      column_delimiter, row_separator, max_row_length, skip_rows, filename_column ? "true" : "false", path_column ? "true" : "false", empty_as_null ? "true" : "false");

}

void CsvFactory::parseColumns(Vertica::ServerInterface &srvInterface, bool debug, char column_delimiter, char row_separator, long max_row_length, columns_t& columns) {
  long column_count = 32;
  if(srvInterface.getParamReader().containsParameter("column_count")) {
    column_count = srvInterface.getParamReader().getIntRef("column_count");
    if(column_count < 1 || column_count > 256)
      vt_report_error(1, "Bad parameter column_count %li out of range 1..256", column_count);
  }

  std::string column_names;
  if(srvInterface.getParamReader().containsParameter("column_names")) {
    column_names = srvInterface.getParamReader().getStringRef("column_names").str();
    if(column_names.length() < 1 || column_names.length() > 4096)
      vt_report_error(2, "Bad parameter column_names %s", column_names.c_str());
  }

  std::string file_with_header;
  if(srvInterface.getParamReader().containsParameter("file_with_header")) {
    file_with_header = srvInterface.getParamReader().getStringRef("file_with_header").str();
  }

  if(!column_names.empty() && !file_with_header.empty())
    vt_report_error(8, "Cannot specify column_names and file_with_header together");

  if(debug)
    srvInterface.log("Parameters column_count %li column_names %s file_with_header %s",
      column_count, column_names.c_str(), file_with_header.c_str());

  std::string column_string;
  if(column_names.length() != 0) {
    column_string = column_names;
  }

  if(file_with_header.length() != 0) {
    std::ifstream header(file_with_header);
    if (!header.is_open())
      vt_report_error(9, "Error opening header file %s", file_with_header.c_str());
    if(!std::getline(header, column_string, row_separator))
      vt_report_error(10, "Error reading line from header file %s", file_with_header.c_str());
    if(column_string.empty())
      vt_report_error(11, "Zero length header line in file %s", file_with_header.substr(1,256).c_str());
    if(column_string.back() == '\r')
      column_string.pop_back();
    if((column_string.empty()) || column_string.length() > static_cast<size_t>(max_row_length))
      vt_report_error(11, "Bad header line %s", file_with_header.substr(1,256).c_str());
  }


  if(column_string.empty()){
    for (int i = 0; i < column_count; i++) {
      columns.push_back("col" + std::to_string(i + 1));
    }
  } else {
    size_t ns = 0;
    size_t ne = 0;
    while(ne != std::string::npos) {
      ne = column_string.find(column_delimiter, ns);
      columns.push_back(column_string.substr(ns, (ne == std::string::npos) ? std::string::npos : ne - ns));
      ns = ( ( ne == std::string::npos) ? std::string::npos : ne + 1);
    }
  }
}

void Csv::setup(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes){
  columns_t columns;

  CsvFactory::parse_parameters(srvInterface, debug, column_delimiter, row_separator, max_row_length, skip_rows, filename_column, path_column, empty_as_null);

  CsvFactory::parseColumns(srvInterface, debug, column_delimiter, row_separator, max_row_length, columns);
  column_count = static_cast<long>(columns.size());

  if(debug)
    srvInterface.log("Setup column_count %li", column_count);
}

void Csv::processPartition(Vertica::ServerInterface &srvInterface, Vertica::PartitionReader &inputReader, Vertica::PartitionWriter &outputWriter) {
  UDxUtilities::check_argument_count(inputReader, 1);
  try {
    do {
      std::string filename = inputReader.getStringRef(0).str();
      std::ifstream file(filename);
      if (!file.is_open())
        vt_report_error(12, "Error opening file %s", filename.c_str());
      size_t line_no = 0;
      std::string line;
      size_t last_slash = filename.find_last_of('/');
      std::string filename_file = filename.substr(last_slash + 1);
      std::string filename_path = filename.substr(0, last_slash + 1);
      std::string file_line;
      while(std::getline(file, file_line, row_separator)) {
        if(file_line.length() > static_cast<size_t>(max_row_length))
          vt_report_error(14, "Row length in file %i exceeds max_row_length %i", file_line.length(), max_row_length);
        line_no++;
        if(line_no%10000 == 0) {
          if(isCanceled()) {
            if(debug)
              srvInterface.log("Got cancelled");
            return;
          }
        }
        if(line_no <= static_cast<size_t>(skip_rows))
          continue;
        long col_ind = 0;

        if(file_line.back() == '\r')
          file_line.pop_back();

        std::vector<std::string> tokens;
        size_t ns = 0;
        size_t ne = 0;
        while(ne != std::string::npos) {
          ne = file_line.find(column_delimiter, ns);
          tokens.push_back(file_line.substr(ns, (ne == std::string::npos) ? std::string::npos : ne - ns));
          ns = ( ( ne == std::string::npos) ? std::string::npos : ne + 1);
        }

        for (const auto& t : tokens) {
          if(empty_as_null && t.empty()) {
            outputWriter.setNull(col_ind);
          } else {
            outputWriter.getStringRef(col_ind).copy(t);
          }
          col_ind++;
          if(col_ind >= column_count)
            break;
        }
        if(col_ind < column_count) {
          for(long i = col_ind; i < column_count; i++)
            outputWriter.setNull(i);
          col_ind = column_count;
        }

        if(filename_column) {
          outputWriter.getStringRef(col_ind).copy(filename_file);
          col_ind++;
        }
        if(path_column) {
          outputWriter.getStringRef(col_ind).copy(filename_path);
        }
        outputWriter.next();
      }
      if(debug)
        srvInterface.log("Read file %s %li lines", filename.c_str(), line_no);
    } while ( inputReader.next() );
  } catch(const std::exception& e) {
      vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(CsvFactory);

} // namespaces VerticaUDxUtilities

