#include <fstream>
#include "VerticaLog.h"
#include "UDxUtilities.h"
#include <filesystem>

namespace VerticaUDxUtilities {

constexpr size_t date_ci = 0;
constexpr size_t time_ci = 1;
constexpr size_t node_name_ci = 2;

constexpr size_t call_ci = 3;
constexpr size_t module_ci = 4;
constexpr size_t process_ci = 5;
constexpr size_t txn_ci = 6;
constexpr size_t group_ci = 7;
constexpr size_t level_ci = 8;
constexpr size_t body_ci = 9;

constexpr size_t udx_lang_ci = 3;
constexpr size_t udx_n_name_ci = 4;
constexpr size_t udx_pid1_ci = 5;
constexpr size_t udx_pid2_ci = 6;
constexpr size_t udx_pid3_ci = 7;
constexpr size_t udx_process_ci = 8;
constexpr size_t udx_group_ci = 9;
constexpr size_t udx_function_ci = 10;
constexpr size_t udx_body_ci = 11;

void VerticaLogFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  returnType.addAny();
}

void VerticaLogFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 0);
  Vertica::SizedColumnTypes::Properties prop_sorted;
  prop_sorted.isSortedBy = true;
  outputTypes.addVarchar(10, "date", prop_sorted);
  outputTypes.addVarchar(12, "time", prop_sorted);
  outputTypes.addVarchar(32, "node_name");
  if(srvInterface.getParamReader().containsParameter("udx")) {
    outputTypes.addVarchar(16, "lang");
    outputTypes.addVarchar(20, "n_name");
    outputTypes.addVarchar(32, "pid1");
    outputTypes.addVarchar(16, "pid2");
    outputTypes.addVarchar(16, "pid3");
    outputTypes.addVarchar(16, "process");
    outputTypes.addVarchar(16, "group");
    outputTypes.addVarchar(32, "function");
    outputTypes.addVarchar(256, "body");
  } else {
    outputTypes.addVarchar(32, "call");
    outputTypes.addVarchar(32, "module");
    outputTypes.addVarchar(32, "process");
    outputTypes.addVarchar(32, "txn");
    outputTypes.addVarchar(32, "group");
    outputTypes.addVarchar(32, "level");
    outputTypes.addVarchar(256, "body");
  }
}

void VerticaLogFactory::getParameterType(Vertica::ServerInterface &srvInterface, Vertica::SizedColumnTypes &parameterTypes){
  parameterTypes.addVarchar(32, "node");
  parameterTypes.addVarchar(8, "date");
  parameterTypes.addVarchar(64, "catalog_dir");
  parameterTypes.addVarchar(64, "udx");
  parameterTypes.addInt("last_mb");
  parameterTypes.addVarchar(64, "debug");
  parameterTypes.addVarchar(64, "dummy");
  parameterTypes.addVarchar(10, "log_date");
  parameterTypes.addVarchar(12, "time_start");
  parameterTypes.addVarchar(12, "time_end");
  parameterTypes.addVarchar(20, "txn");
  parameterTypes.addVarchar(256, "contains");
  parameterTypes.addVarchar(256, "contains2");
  parameterTypes.addVarchar(256, "exclude");
}

Vertica::TransformFunction * VerticaLogFactory::createTransformFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, VerticaLog);
}

VerticaLog::VerticaLog() :
log_file_exist(false),
node_match(false),
last_mb(0),
udx(false),
log_date_flag(false),
time_started(false),
time_start_flag(false),
time_end_flag(false),
txn_flag(false),
contains_flag(false),
contains2_flag(false),
exclude_flag(false),
debug(false)
{}

std::string VerticaLog::getParamStr(const Vertica::ParamReader& params, const std::string& param_name) {
  return params.containsParameter(param_name) ? params.getStringRef(param_name).str() : "";
}

void VerticaLog::getParamStr(const Vertica::ParamReader& params, const std::string& param_name, std::string& out_var, bool& out_flag) {
  out_var = getParamStr(params, param_name);
  out_flag = !out_var.empty();
}

void VerticaLog::setup(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes & 	argTypes ) {
  current_node_name = srvInterface.getCurrentNodeName();
  const Vertica::ParamReader& params = srvInterface.getParamReader();
  node = getParamStr(params, "node");
  date = getParamStr(params, "date");
  if(!date.empty() && date.length() != 8) {
    vt_report_error(101, "Bad format for parameter date - expected YYYYMMDD, got %s", date.c_str());
  }
  catalog_dir = getParamStr(params, "catalog_dir");
  if(catalog_dir.empty()) {
    log_file = "/vertica/" + srvInterface.getDatabaseName();
  } else {
    log_file = catalog_dir;
  }
  udx = params.containsParameter("udx");
  if(udx) {
    log_file += "/" + current_node_name + "_catalog/UDxLogs/UDxFencedProcesses.log";
  } else {
    log_file += "/" + current_node_name + "_catalog/vertica.log";
  }
  if (!date.empty()) {
    log_file += "-" + date + ".gz";
  }
  getParamStr(params, "log_date", log_date, log_date_flag);
  if(log_date_flag && (log_date.length() != 10 || log_date[0] != '2' || log_date[1] != '0' || log_date[4] != '-' || log_date[7] != '-')) {
    vt_report_error(101, "Bad format for parameter log_date - expected YYYY-MM-DD, got %s", log_date.c_str());
  }
  getParamStr(params, "time_start", time_start, time_start_flag);
  getParamStr(params, "time_end", time_end, time_end_flag);
  if(!time_start.empty() && !time_end.empty() && time_start.compare(time_end) > 0) {
    vt_report_error(112, "Time_start %s is greater than time_end %s", time_start.c_str(), time_end.c_str());
  }
  getParamStr(params, "txn", txn, txn_flag);
  getParamStr(params, "contains", contains_param, contains_flag);
  getParamStr(params, "contains2", contains2_param, contains2_flag);
  getParamStr(params, "exclude", exclude_param, exclude_flag);
  debug = params.containsParameter("debug");
  last_mb = (params.containsParameter("last_mb") ? std::max(params.getIntRef("last_mb"), static_cast<Vertica::vint>(0L)): 0);
  log_file_exist =  std::filesystem::is_regular_file(log_file);
  node_match = node.empty() || current_node_name.substr(current_node_name.length() - node.length()) == node;
  if(debug) {
    srvInterface.log("Node %s date %s catalog_dir %s udx %s log_date %s time_start %s time_end %s txn %s contains %s contains2 %s log_file %s log_file_exist %s node_match %s udx %s last_mb %ld",
                     node.c_str(), date.c_str(), catalog_dir.c_str(), (udx ? "True" : "False"),
                     log_date.c_str(), time_start.c_str(), time_end.c_str(), txn.c_str(), contains_param.c_str(), contains2_param.c_str(),
                     log_file.c_str(), (log_file_exist ? "True" : "False"),
                     (node_match ? "True" : "False"), (udx ? "True" : "False"), last_mb);
    std::map<std::string, std::string> event;
    event["node"] = node;
    event["date"] = date;
    event["log_file"] = log_file;
    event["log_file_exist"] = (log_file_exist ? "True" : "False");
    event["node_match"] = (node_match ? "True" : "False");
    srvInterface.logEvent(event);
  }
}

void VerticaLog::getNextToken(const char* str_ptr, size_t str_len, char delimiter, size_t& from_ind, size_t& token_len) {
  const char* f_ptr;
  while(from_ind < str_len) {
    f_ptr = static_cast<const char *>(std::memchr(str_ptr + from_ind, delimiter, str_len - from_ind));
    if(f_ptr == nullptr) {
      token_len = 0;
      return;
    }
    token_len = f_ptr - str_ptr - from_ind;
    if(token_len > 0) {
      return;
    }
    from_ind++;
  }
  token_len = 0;
}

void VerticaLog::prepareProcess(Vertica::ServerInterface &srvInterface) {
  inp.open(log_file);
  if(!inp.is_open()) {
    vt_report_error(101, "Error opening log file %s", log_file.c_str());
  }
  if (!date.empty()) {
    vt_report_error(101, "Reading gz compressed log files not supported %s", log_file.c_str());
  }

  if(last_mb > 0) {
    if(debug) {
      srvInterface.log("Advancing to last %ld mb", last_mb);
    }
    size_t f_size = std::filesystem::file_size(log_file);
    if (static_cast<ssize_t>(f_size) > last_mb * 1024 * 1024) {
      inp.seekg(static_cast<ssize_t>(f_size) - last_mb * 1024 * 1024);
      if(debug) {
        srvInterface.log("Log file size %ld, advanced to %ld", f_size, static_cast<size_t>(inp.tellg()));
      }
      std::getline(inp, line);
      if(debug) {
        srvInterface.log("After getline advanced to %ld", static_cast<size_t>(inp.tellg()));
      }
    }
  }
}

void VerticaLog::processPartition(Vertica::ServerInterface &srvInterface, Vertica::PartitionReader &inputReader, Vertica::PartitionWriter &outputWriter) {
  do {
    if(!node_match || !log_file_exist) {
      continue;
    }
    prepareProcess(srvInterface);
    time_started = false;

    size_t line_cnt = 0;
    while (std::getline(inp, line)) {
      line_cnt++;
      if (line_cnt % 10000 == 0 && isCanceled()) {
        if(debug) {
          srvInterface.log("Cancelled on line %ld", line_cnt);
        }
        break;
      }

      const char* line_ptr = line.c_str();
      const size_t line_len = line.length();
      size_t t_ind = 0;
      size_t t_len = 0;

      //Date
      getNextToken(line_ptr, line_len, ' ', t_ind, t_len);
      if(t_len == 0) {
        continue;
      }
      if( t_len != 10 || line[t_ind] != '2' || line[t_ind + 1] != '0' || line[t_ind + 4] != '-' || line[t_ind + 7] != '-') {
        continue;
      }
      size_t dt_ind = t_ind;
      size_t dt_len = t_len;
      t_ind += t_len;

      if(log_date_flag && std::strncmp(line_ptr, log_date.c_str(), 10) < 0) {
        continue;
      }
      // Time
      getNextToken(line_ptr, line_len, ' ', t_ind, t_len);
      if(t_len == 0) {
        continue;
      }
      if( t_len != 12 || line[t_ind + 2] != ':' || line[t_ind + 5] != ':' || line[t_ind + 8] != '.') {
        continue;
      }
      size_t tm_ind = t_ind;
      size_t tm_len = t_len;

      t_ind += t_len;
      getNextToken(line_ptr, line_len, ' ', t_ind, t_len);
      if(t_len == 0) {
        continue;
      }
      if(time_start_flag && !time_started) {
        if (std::strncmp(line_ptr + tm_ind, time_start.c_str(), std::min(time_start.length(), tm_len)) >= 0) {
          srvInterface.log("Time %s over time_start %s", line.substr(tm_ind, tm_len).c_str(), time_start.c_str());
          time_started = true;
        } else {
          continue;
        }
      }
      if(!(time_start_flag && !time_started) && time_end_flag && std::strncmp(line_ptr + tm_ind, time_end.c_str(), std::min(time_end.length(), tm_len)) > 0) {
        srvInterface.log("Time %s over time_end %s", line.substr(tm_ind, tm_len).c_str(), time_end.c_str());
        break;
      }
      outputWriter.getStringRefNoClear(date_ci).copy(line_ptr + dt_ind, dt_len);
      outputWriter.getStringRefNoClear(time_ci).copy(line_ptr + tm_ind, tm_len);
      outputWriter.getStringRefNoClear(node_name_ci).copy(current_node_name);
      if(contains_flag && line.find(contains_param) == std::string::npos) {
        continue;
      }
      if(contains2_flag && line.find(contains2_param) == std::string::npos) {
        continue;
      }
      if(exclude_flag && line.find(exclude_param) != std::string::npos) {
        continue;
      }
      if(udx) {
        if (t_len < 10 || *(line_ptr + t_ind) != '[' || *(line_ptr + t_ind + t_len - 1) != ']') {
          continue;
        }
        t_ind++;
        t_len -= 2;

        size_t l_ind = t_ind;
        size_t l_len = 0;
        getNextToken(line_ptr, t_ind + t_len, '-', l_ind, l_len);
        if (l_len == 0) {
          continue;
        }

        size_t n_ind = l_ind + l_len + 1;
        size_t n_len = 0;
        getNextToken(line_ptr, t_ind + t_len, '-', n_ind, n_len);
        if (n_len == 0) {
          continue;
        }

        size_t pid1_ind = n_ind + n_len + 1;
        size_t pid1_len = 0;
        getNextToken(line_ptr, t_ind + t_len, ':', pid1_ind, pid1_len);

        if (pid1_len == 0) {
          continue;
        }

        size_t pid2_ind = pid1_ind + pid1_len + 1;
        size_t pid2_len = 0;
        getNextToken(line_ptr, t_ind + t_len, '-', pid2_ind, pid2_len);
        if (pid2_len == 0) {
          continue;
        }
        size_t pid3_ind = pid2_ind + pid2_len + 1;
        size_t pid3_len = t_len - l_len - n_len - pid1_len - pid2_len - 4;
        if (pid3_len == 0) {
          continue;
        }
        outputWriter.getStringRefNoClear(udx_lang_ci).copy(line_ptr + l_ind, l_len);
        outputWriter.getStringRefNoClear(udx_n_name_ci).copy(line_ptr + n_ind, n_len);
        outputWriter.getStringRefNoClear(udx_pid1_ci).copy(line_ptr + pid1_ind, pid1_len);
        outputWriter.getStringRefNoClear(udx_pid2_ci).copy(line_ptr + pid2_ind, pid2_len);
        outputWriter.getStringRefNoClear(udx_pid3_ci).copy(line_ptr + pid3_ind, pid3_len);

        t_ind += t_len + 2;

        size_t pr_ind = t_ind;
        size_t pr_len = 0;
        getNextToken(line_ptr, line_len, ' ', pr_ind, pr_len);
        if(pr_len == 0) {
          continue;
        }
        outputWriter.getStringRefNoClear(udx_process_ci).copy(line_ptr + pr_ind, pr_len);
        t_ind = pr_ind + pr_len;

        size_t gr_ind = t_ind;
        size_t gr_len = 0;
        getNextToken(line_ptr, line_len, ' ', gr_ind, gr_len);
        if(gr_len < 3 || *(line_ptr + gr_ind) !=  '[' || *(line_ptr + gr_ind + gr_len - 1) !=  ']') {
          outputWriter.getStringRefNoClear(udx_group_ci).setNull();
          outputWriter.getStringRefNoClear(udx_function_ci).setNull();
        } else {
          outputWriter.getStringRefNoClear(udx_group_ci).copy(line_ptr + gr_ind + 1, gr_len - 2);
          t_ind += gr_len + 1;

          size_t f_ind = t_ind;
          size_t f_len = 0;
          getNextToken(line_ptr, line_len, ' ', f_ind, f_len);
          if (f_len < 1) {
            continue;
          }
          outputWriter.getStringRefNoClear(udx_function_ci).copy(line_ptr + f_ind, f_len);
          t_ind = f_ind + f_len + 2;
        }
        outputWriter.getStringRefNoClear(udx_body_ci).copy(line_ptr + t_ind, std::min(256UL, line_len - t_ind));
      } else {
        if ((t_len == 8 && std::strncmp(line_ptr + t_ind, "DistCall", 8) == 0)
            || (t_len == 2 && std::strncmp(line_ptr + t_ind, "TM", 2) == 0)
            || (t_len == 4 && std::strncmp(line_ptr + t_ind, "Init", 4) == 0)) {
          outputWriter.getStringRefNoClear(call_ci).copy(line_ptr + t_ind, t_len);
          t_ind += t_len;
          getNextToken(line_ptr, line_len, ' ', t_ind, t_len);
        } else {
          outputWriter.getStringRef(call_ci).setNull();
        }

        size_t p_ind = t_ind;
        size_t p_len = 0;
        getNextToken(line_ptr, t_ind + t_len, ':', p_ind, p_len);
        if (p_len > 0) {
          outputWriter.getStringRefNoClear(module_ci).copy(line_ptr + t_ind, p_len);
          size_t txn_ind = p_ind + p_len + 1;
          size_t txn_len = 0;
          getNextToken(line_ptr, t_ind + t_len, '-', txn_ind, txn_len);
          if (txn_len > 0) {
            if(txn_flag && (txn.length() != t_len - txn_len - p_len - 2 || strncmp(txn.c_str(), line_ptr + txn_ind + txn_len + 1, t_len - txn_len - p_len - 2) != 0)) {
              continue;
            }
            outputWriter.getStringRefNoClear(process_ci).copy(line_ptr + txn_ind, txn_len);
            outputWriter.getStringRefNoClear(txn_ci).copy(line_ptr + txn_ind + txn_len + 1, t_len - txn_len - p_len - 2);
          } else {
            if(txn_flag) {
              continue;
            }
            outputWriter.getStringRefNoClear(process_ci).copy(line_ptr + t_ind + p_len + 1, t_len - p_len - 1);
            outputWriter.getStringRef(txn_ci).setNull();
          }
        } else {
          if(txn_flag) {
            continue;
          }
          outputWriter.getStringRefNoClear(module_ci).copy(line_ptr + t_ind, t_len);
          outputWriter.getStringRef(process_ci).setNull();
          outputWriter.getStringRef(txn_ci).setNull();
        }
        t_ind += t_len;

        getNextToken(line_ptr, line_len, ' ', t_ind, t_len);
        if (t_len == 0) {
          if (debug) {
            srvInterface.log("No token found after module");
          }
          continue;
        }
        if (t_len >= 3 && *(line_ptr + t_ind) == '[' && *(line_ptr + t_ind + t_len - 1) == ']') {
          outputWriter.getStringRefNoClear(group_ci).copy(line_ptr + t_ind + 1, t_len - 2);
          t_ind += t_len;
          getNextToken(line_ptr, line_len, ' ', t_ind, t_len);
        } else {
          outputWriter.getStringRef(group_ci).setNull();
        }

        if (t_len == 0) {
          if (debug) {
            srvInterface.log("No token found after brackets");
          }
          continue;
        }

        if (t_len >= 3 && *(line_ptr + t_ind) == '<' && *(line_ptr + t_ind + t_len - 1) == '>') {
          outputWriter.getStringRefNoClear(level_ci).copy(line_ptr + t_ind + 1, t_len - 2);
          t_ind += t_len;
        } else {
          outputWriter.getStringRef(level_ci).setNull();
        }
        outputWriter.getStringRefNoClear(body_ci).copy(line_ptr + t_ind, std::min(256UL, line_len - t_ind));
      }
      outputWriter.next();
    }
    inp.close();
  } while ( inputReader.next() );
}

RegisterFactory(VerticaLogFactory);

void VerticaLogSizeFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  returnType.addVarchar();      // 0 - node name
  returnType.addVarchar();      // 1 - vertica log
  returnType.addInt();          // 2 - size
}

void VerticaLogSizeFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  UDxUtilities::check_argument_count(inputTypes, 0);
  outputTypes.addVarchar(32, "node_name");
  outputTypes.addVarchar(128, "vertica_log");
  outputTypes.addInt("size");
}

void VerticaLogSizeFactory::getParameterType(Vertica::ServerInterface &srvInterface, Vertica::SizedColumnTypes &parameterTypes){
  parameterTypes.addVarchar(32, "node");
  parameterTypes.addVarchar(8, "date");
  parameterTypes.addVarchar(64, "catalog_dir");
  parameterTypes.addVarchar(64, "udx");
  parameterTypes.addVarchar(64, "debug");
}

Vertica::TransformFunction * VerticaLogSizeFactory::createTransformFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, VerticaLogSize);
}

void VerticaLogSize::processPartition(Vertica::ServerInterface &srvInterface, Vertica::PartitionReader &inputReader, Vertica::PartitionWriter &outputWriter) {
  do {
    outputWriter.getStringRefNoClear(0).copy(srvInterface.getCurrentNodeName());
    outputWriter.getStringRefNoClear(1).copy(log_file);
    if(log_file_exist) {
      outputWriter.setInt(2, static_cast<Vertica::vint>(std::filesystem::file_size(log_file)));
    } else {
      outputWriter.setInt(2, Vertica::vint_null);
    }
    outputWriter.next();
  } while ( inputReader.next() );
}

RegisterFactory(VerticaLogSizeFactory);

} // namespaces VerticaUDxUtilities
