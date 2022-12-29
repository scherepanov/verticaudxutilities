DROP LIBRARY DynPythonLib CASCADE;

\set ON_ERROR_STOP on

CREATE or replace LIBRARY DynPythonLib AS :libfile LANGUAGE 'Python';

CREATE or replace FUNCTION dyn_python_varchar   AS LANGUAGE 'Python' NAME 'DynPythonVarchar_factory' LIBRARY DynPythonLib fenced; 
CREATE or replace FUNCTION dyn_python_int       AS LANGUAGE 'Python' NAME 'DynPythonInt_factory' LIBRARY DynPythonLib fenced; 
CREATE or replace FUNCTION dyn_python_float     AS LANGUAGE 'Python' NAME 'DynPythonFloat_factory' LIBRARY DynPythonLib fenced; 
CREATE or replace FUNCTION dyn_python_date      AS LANGUAGE 'Python' NAME 'DynPythonDate_factory' LIBRARY DynPythonLib fenced; 
CREATE or replace FUNCTION dyn_python_timestamp AS LANGUAGE 'Python' NAME 'DynPythonTimestamp_factory' LIBRARY DynPythonLib fenced; 
CREATE or replace FUNCTION dyn_python_time      AS LANGUAGE 'Python' NAME 'DynPythonTime_factory' LIBRARY DynPythonLib fenced; 

CREATE or replace TRANSFORM FUNCTION dyn_python AS LANGUAGE 'Python' NAME 'DynPythonTransform_factory' LIBRARY DynPythonLib fenced;

grant execute on all functions in schema public to public;

\set ON_ERROR_STOP off

\i /tmp/test_queries.sql
