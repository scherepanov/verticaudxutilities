import vertica_sdk
import re

class DynPythonVarchar(vertica_sdk.ScalarFunction):
    def processBlock(self, srv, arg_reader, res_writer):
        code = srv.getParamReader().getString('code')
        compiled_code = compile(code, '<string>', 'exec')
        namespace = dict(srv=srv, arg_reader=arg_reader, res_writer=res_writer)
        exec(compiled_code, namespace)

class DynPythonVarchar_factory(vertica_sdk.ScalarFunctionFactory):
    def getPrototype(self, srv, arg_types, return_type):
        arg_types.addAny()
        return_type.addVarchar()
    def getReturnType(self, srv, arg_types, return_type):
        return_type.addVarchar(64000)
    def getParameterType(self, srv, parameter_types):
        parameter_types.addVarchar(64000, 'code')
    def createScalarFunction(self, srv):
        return DynPythonVarchar()

class DynPythonInt(vertica_sdk.ScalarFunction):
    def processBlock(self, srv, arg_reader, res_writer):
        code = srv.getParamReader().getString('code')
        compiled_code = compile(code, '<string>', 'exec')
        namespace = dict(srv=srv, arg_reader=arg_reader, res_writer=res_writer)
        exec(compiled_code, namespace)

class DynPythonInt_factory(vertica_sdk.ScalarFunctionFactory):
    def getPrototype(self, srv, arg_types, return_type):
        arg_types.addAny()
        return_type.addInt()
    def getReturnType(self, srv, arg_types, return_type):
        return_type.addInt()
    def getParameterType(self, srv, parameter_types):
        parameter_types.addVarchar(64000, 'code')
    def createScalarFunction(self, srv):
        return DynPythonInt()

class DynPythonFloat(vertica_sdk.ScalarFunction):
    def processBlock(self, srv, arg_reader, res_writer):
        code = srv.getParamReader().getString('code')
        compiled_code = compile(code, '<string>', 'exec')
        namespace = dict(srv=srv, arg_reader=arg_reader, res_writer=res_writer)
        exec(compiled_code, namespace)

class DynPythonFloat_factory(vertica_sdk.ScalarFunctionFactory):
    def getPrototype(self, srv, arg_types, return_type):
        arg_types.addAny()
        return_type.addFloat()
    def getReturnType(self, srv, arg_types, return_type):
        return_type.addFloat()
    def getParameterType(self, srv, parameter_types):
        parameter_types.addVarchar(64000, 'code')
    def createScalarFunction(self, srv):
        return DynPythonFloat()
    
class DynPythonDate(vertica_sdk.ScalarFunction):
    def processBlock(self, srv, arg_reader, res_writer):
        code = srv.getParamReader().getString('code')
        compiled_code = compile(code, '<string>', 'exec')
        namespace = dict(srv=srv, arg_reader=arg_reader, res_writer=res_writer)
        exec(compiled_code, namespace)

class DynPythonDate_factory(vertica_sdk.ScalarFunctionFactory):
    def getPrototype(self, srv, arg_types, return_type):
        arg_types.addAny()
        return_type.addDate()
    def getReturnType(self, srv, arg_types, return_type):
        return_type.addDate()
    def getParameterType(self, srv, parameter_types):
        parameter_types.addVarchar(64000, 'code')
    def createScalarFunction(self, srv):
        return DynPythonDate()

class DynPythonTimestamp(vertica_sdk.ScalarFunction):
    def processBlock(self, srv, arg_reader, res_writer):
        code = srv.getParamReader().getString('code')
        compiled_code = compile(code, '<string>', 'exec')
        namespace = dict(srv=srv, arg_reader=arg_reader, res_writer=res_writer)
        exec(compiled_code, namespace)

class DynPythonTimestamp_factory(vertica_sdk.ScalarFunctionFactory):
    def getPrototype(self, srv, arg_types, return_type):
        arg_types.addAny()
        return_type.addTimestamp()
    def getReturnType(self, srv, arg_types, return_type):
        return_type.addTimestamp()
    def getParameterType(self, srv, parameter_types):
        parameter_types.addVarchar(64000, 'code')
    def createScalarFunction(self, srv):
        return DynPythonTimestamp()

class DynPythonTime(vertica_sdk.ScalarFunction):
    def processBlock(self, srv, arg_reader, res_writer):
        code = srv.getParamReader().getString('code')
        compiled_code = compile(code, '<string>', 'exec')
        namespace = dict(srv=srv, arg_reader=arg_reader, res_writer=res_writer)
        exec(compiled_code, namespace)

class DynPythonTime_factory(vertica_sdk.ScalarFunctionFactory):
    def getPrototype(self, srv, arg_types, return_type):
        arg_types.addAny()
        return_type.addTime()
    def getReturnType(self, srv, arg_types, return_type):
        return_type.addTime()
    def getParameterType(self, srv, parameter_types):
        parameter_types.addVarchar(64000, 'code')
    def createScalarFunction(self, srv):
        return DynPythonTime()


class DynPythonTransform(vertica_sdk.TransformFunction):
    def processPartition(self, srv, arg_reader, res_writer):
        try:
            self.compiled_code
        except AttributeError as e:
            column_code=''
            copy_columns = srv.getParamReader().containsParameter('copy_columns')
            metadata = arg_reader.getTypeMetadata()
            for arg_col_ind in range(metadata.getColumnCount()):
                column_code = column_code + '_s_' + metadata.getColumnName(arg_col_ind) + '=' + str(arg_col_ind) + '\n'
    
            if copy_columns:
                res_col_ind = metadata.getColumnCount()
            else:
                res_col_ind = 0

            if srv.getParamReader().containsParameter('result_columns'):
                result_columns = srv.getParamReader().getString('result_columns').split()
                for field in result_columns:
                    column_code = column_code + '_r_' + field.split(':')[0] + '=' + str(res_col_ind) + ';\n'
                    res_col_ind += 1
 
            column_code = column_code + '_r_column_count' + '=' + str(res_col_ind) + ";\n";
            column_code = column_code + '_s_column_count' + '=' + str(metadata.getColumnCount()) + ";\n"
        
            code = column_code + srv.getParamReader().getString('code')
            self.compiled_code = compile(code, '<string>', 'exec')

        namespace = dict(srv=srv, arg_reader=arg_reader, res_writer=res_writer)
        exec(self.compiled_code, namespace)

class DynPythonTransform_factory(vertica_sdk.TransformFunctionFactory):
    def getPrototype(self, srv, arg_types, return_type):
        arg_types.addAny()
        return_type.addAny()
    def getReturnType(self, srv, arg_types, return_type):
        if not srv.getParamReader().containsParameter('result_columns') and \
            not srv.getParamReader().containsParameter('copy_columns'):
            raise ValueError('At least one of parameters result_columns copy_columns must be specified')

        if srv.getParamReader().containsParameter('copy_columns'):
            for ind in range(arg_types.getColumnCount()):
                arg_name = arg_types.getColumnName(ind)
                if not arg_name:
                    arg_name = "__c" + str(ind) + "__"     
                arg_type = arg_types.getColumnType(ind)
                if arg_type.isBool():
                    return_type.addBool(arg_name)
                elif arg_type.isChar():
                    return_type.addChar(arg_type.getStringLength(), arg_name)
                elif arg_type.isFloat():
                    return_type.addFloat(arg_name)
                elif arg_type.isInt():
                    return_type.addInt(arg_name)
                elif arg_type.isNumeric():
                    return_type.addNumeric(arg_name)
                elif arg_type.isDate():
                    return_type.addDate(arg_name)
                elif arg_type.isTime():
                    return_type.addTime(arg_name)
                elif arg_type.isTimestamp():
                    return_type.addTimestamp(arg_name)
                elif arg_type.isVarchar():
                    return_type.addVarchar(arg_type.getStringLength(), arg_name)
                else:
                    raise ValueError('Copy of unsupported argument of type ' + arg_type.getPrettyPrintStr())
        if srv.getParamReader().containsParameter('result_columns'):
            result_columns = srv.getParamReader().getString('result_columns').split()
            column_ind = 0
            for field in result_columns:
                col_def = field.split(':')
                if len(col_def) != 2 or len(col_def[0]) == 0 or len(col_def[1]) == 0:
                    raise ValueError('Bad format of return type ' + field)
                column_name = col_def[0]
                column_type = col_def[1].lower()
                if(len(column_name) == 0 or len(column_type) == 0):
                    raise ValueError("Bad format of result column " + field)
                if column_type == 'string':
                    return_type.addVarchar(64000, column_name)
                elif column_type == 'int':
                    return_type.addInt(column_name)
                elif column_type == 'float':
                    return_type.addFloat(column_name)
                elif column_type == 'numeric':
                    return_type.addNumeric(column_name, 18,9)
                elif column_type == 'date':
                    return_type.addDate(column_name)
                elif column_type == 'time':
                    return_type.addTime(column_name)
                elif column_type == 'timestamp':
                    return_type.addTimestamp(column_name)
                else:
                    raise ValueError('Invalid type of result column ' + column_type + ' in ' + field)        


    def getParameterType(self, srv, parameter_types):
        parameter_types.addVarchar(64000, 'code')
        parameter_types.addVarchar(2048, 'result_columns')
        parameter_types.addBool('copy_columns');
    def createTransformFunction(self, srv):
        return DynPythonTransform()

