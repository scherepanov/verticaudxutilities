# Dynamit Python execution in Vertica

SQL is a very powerful language. Prime power comes from ability to express set operations.

This power comes with a weak side. Very large class of operations not possible, or extremely hard, or very inefficient to express using SQL.

On other side, using programming languages you can express pretty much anything. That comes with a cost of ... (reader can put your favorite arguments here).

Generally speaking, Vertica can do only SQL

Here is how Vertica you can do arbitrary code execution - write code straight inside SQL and have it dynamically executed.

Example is for Python, but Java will work same way (and would be much faster and powerful than Python).

## General idea
```
select dyn_python(
<Your python code>
)
from (
select <columns> from <mytable> where <my conditions>
)
```

You select data from your table in Vertica, write some Python code, and it is being executed inside Vertica, consuming source data and producing destination data.

Arbitrarily complex programing logic allowed there.

Dynamic Python output looks like a normal table - you can use it as one of join tables in your SQL.

## Example
Thinking of best illustration what you can easily express in Python but not possible in SQL, I come to following:

Source table have 2 columns - in_str varchar(128), delim char(1). Column in_str contains a phrase, delim contains a single-char delimiter.

Split in_str by delimiter, sort, and join back into string.

I do not know about any solution of this problem in Vertica SQL (Note that was before Vertica had Arrays).

Here is a solution using Dynamic Python:
```p
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
) as sorted_string from
(select 'ani beni liki poki trul bul buli kaluki shmoki' in_str, ' ' delim
union all
select 'Quick fox jumps over lazy dog back', ' '
union all
select 'f,a,b,c,e,d,e,f', ','  
) v;
sorted_string
------------------------------------------------
ani beni bul buli kaluki liki poki shmoki trul
Quick back dog fox jumps lazy over
a,b,c,d,e,e,f,f
(3 rows)
```
As you can see, use of right tool - Python - solves problem nice and easy.

###Explanations
Part below is used to emulate 2 columns table (in_str varchar(128), delim char(1)) with 3 rows
```
select 'ani beni liki poki trul bul buli kaluki shmoki' in_str, ' ' delim
union all
select 'Quick fox jumps over lazy dog back', ' '
union all
select 'f,a,b,c,e,d,e,f', ','
```
Python code are supplied at runtime, dynamically executed.

We are receiving data from Vertica using input iterable arg_reader. You need to fetch all columns for each row by index that starts with 0. 
You can have any number and types of input argument to dynamic python functions, it is your responsibility to read all of them for each row.

Using read data, we are doing calculations - split, sort, join. Python rocks!

We are sending output to Vertica using iterable writer res_writer, with single column of type varchar (can be more output columns of different types, see below).

Of course we are handling input null data, and paying our dues to the fact that arg_reader and res_writer are iterable (i.e. calling/checking next()).

```
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
```
That is a skeleton of any programmatic calculation inside Vertica.

###Keeping state between rows
Dynamic Python allow you to keep internal state between execution of rows. For example, you can calculate Fibonacci sequence.

Function seq(1,10) is being used to generate 10 rows with sequential number. 
We are not using output of this function, we need 10 rows, and this function provide us with 10 rows.
```
select dyn_python_int(v.* using parameters
code='
before_prev_num = 0;
prev_num = 1;
iteration = 0
while True:
if iteration < 2:
res_writer.setInt(iteration)   
else:    
fibonacci_sequence = before_prev_num + prev_num
res_writer.setInt(fibonacci_sequence)   
before_prev_num = prev_num
prev_num = fibonacci_sequence
res_writer.next()
if not arg_reader.next():
break
iteration = iteration + 1
'
) from
(select seq(1,10) over()) v;
```
### Dynamic Python implementation details
There are two types of functions - scalar and transform.

####Dynamic Python Scalar functions
Scalar functions acts like a regular SQL scalar functions - can consume many arguments of varying types, can produce single result of one type, and have to produce one row output per each input row.

Can be used same way as regular SQL scalar functions.

Due to a fact that return of scalar function is typed, there is a set of 7 functions that differ only by output type and have identical syntax
```
dyn_python_date
dyn_python_float
dyn_python_int
dyn_python_test
dyn_python_time
dyn_python_timestamp
dyn_python_varchar
```
Syntax:
```
select dyn_python_<output_type>(arg1, [argx] using parameters code = '
<Python code goes here>
') from xxxxxxxx;
```
You should supply at least one argument to scalar function.

Python code should start with 0 indentation.

You Python code should use correct reader for each input argument (like getString, getInt, getBool, getFloat, getDate, getTime, getTimestamp) and specify correct index of input column, starting with 0.

Output writer should be of correct type (setString, setInt, setFloat, setDate, setTime, setTimestamp). As scalar functions have single output column, column index is not needed.

### Dynamic Python Transform function
Transform function for each input row can produce zero, one or many output rows.

Transform function can have many named output columns of different types.

Transform function can have one or more input arguments.

As transform functions are universal, there is a single Dynamic Python transform function aptly named dyn_python

#### Transform example

Lets slightly complicate (or simplify) our problem - instead of joining, we will output one token per row:
```
select Dyn_Python('ani beni liki poki trul bul bul kaluki shmoki', ' '
using parameters
result_columns = '
ret:string
src:string',
code='
while True:
if arg_reader.isNull(0):
res_writer.setNull(0)
res_writer.setNull(1)
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
ret
--------
ani
beni
bul
bul
kaluki
liki
poki
shmoki
trul
(9 rows)
```
#### Difference between transform and scalar functions:
You need to specify list of return columns - their names and types, in parameter return_types. You specify space-delimited list of <column_name>:<column_type>.

Allowed output column types are string, int, flat, date, time, timestamp. If you need other Veritca types (numeric, bool, varbinary etc) let me know, I will add.

Example of return_types parameter string - symbol:string ask_price:float bid_price:fload ask_count:int bid_count:int is_trade:bool quote_time:timestamp trade_date:date

You need to use correct output setter for each column (setString, setInt, setFloat, setDate, setTime, setTimestamp) and column index.

You need to specify over() clause.

Note that over() can have partitioning and order clauses inside, which can be very powerful, same as for Vertica analytical functions.

Transform function can be the only one in select clause.

setNull now takes parameter - return column index

#### More examples
Let's copy all column from input to output, and add to output one column with int datatype and value - number of columns in source. We will use wildcard for source columns.
```
select dyn_python(v.* using parameters
copy_columns='y',
result_columns='c_cnt:int',
code='
while True:
for ind in range(_s_column_count):
res_writer.copyFromInput(ind, arg_reader, ind)
res_writer.setInt(_r_c_cnt, _r_column_count)    
res_writer.next()    
if not arg_reader.next():
break
'
) over() from
(select 'a' cl1, 1 cl2, sysdate cur_dt) v;
```
We added to output all input columns (copy_columns='y'), and added to output column c_int of type int (result_columns='c_int:int').

We have variables _s_column_count and _r_column_count, they contain number of columns in input (_s_column_count) and output (_r_column_count).

They are being used to loop through all input columns.

To access additional output column c_int, we are using variable _r_c_int that contains index of this column. For each input column, we have column index in variables _s_<input_column_name>. If you request copy of input columns to output, output column index will be same as input, you can use _s_<input_column_name> (input and output column name is same when you copy_columns). For additional output columns, you have variables _r_<output_column_name> with their index. They are for use in column setters and getters, they are taking column index as first argument.

Additional output column c_int is set with number of columns in output.

### Other notes
- Transform function is compiling Python code for each partition, which can be a performance hit.

- You can use Python raise ValueError(<my text>) exception to stop SQL execution and report error to user

- If you need to import some module, do it in first line of Python code snippet

- Indent from 0 your Python

- If you need more complicated program, moving to dynamic Java would be a good idea, due to muuch better performance and full-blown functionality

Internal Vertica code that implements Dynamic Python is 19 lines, 2 of them are blank lines

## Conclusion
Dynamic programming interface is very nicely compliment expression power of SQL, and allow to express problems that are not possible with set-oriented SQL.

Correct use of SQL and Dynamic Python mix can significantly speed up SQL execution and greatly simplify SQL text.
