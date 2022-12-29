SELECT dyn_python_varchar('Z,S,A,X,C,B' using parameters code='
while True:
    if arg_reader.isNull(0):
        res_writer.setNull()
        continue
    res_writer.setString(arg_reader.getString(0))
    res_writer.next()
    if not arg_reader.next():
        break
');

SELECT dyn_python_varchar('Z,S,A,X,C,B' using parameters code='
while True:    
    res_writer.setString(arg_reader.getString(0))
    res_writer.next()
    if not arg_reader.next():
        break
');



SELECT dyn_python_int('Z,S,A,X,C,B' using parameters code='
while True:
    res_writer.setInt(len(arg_reader.getString(0)))
    res_writer.next()
    if not arg_reader.next():
        break
');


SELECT dyn_python('Z,S,A,X,C,B' using parameters 
result_columns='c1:string',
code='
while True:
    res_writer.setString(0, arg_reader.getString(0))
    res_writer.next()
    if not arg_reader.next():
        break
') over();

select Dyn_Python_varchar(v.in_str, v.delim using parameters 
code='
while True:
    if arg_reader.isNull(0):
        res_writer.setNull()
        continue
    source = arg_reader.getString(0)
    delimiter = arg_reader.getString(1)
    arr = source.split(delimiter)
    arr = sorted(arr)
    z = delimiter.join(arr)
    res_writer.setString(z)
    res_writer.next()
    if not arg_reader.next():
        break
'
) from
(select 'ani beni liki poki trul bul buli kaluki shmoki' in_str, ' ' delim
  union all
 select 'Quick fox jumps over lazy dog back', ' '
  union all
 select 'f,a,b,c,e,d,e,f', ','  
) v
;

select Dyn_Python('ani beni liki poki trul bul bul kaluki shmoki', ' ' 
using parameters 
result_columns = '
ret:string
scr:string
',
code='
while True:
    if arg_reader.isNull(0):
        res_writer.setNull()
        continue
    source = arg_reader.getString(0)
    delimiter = arg_reader.getString(1)
    arr = source.split(delimiter)
    arr = sorted(arr)
    for token in arr:
        res_writer.setString(0, token)
        res_writer.setString(1, source)
        res_writer.next()
    if not arg_reader.next():
        break
'
) over();


