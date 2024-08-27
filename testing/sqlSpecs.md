# SQL format and syntax

## Select

Format:
    SELECT *column1, column2, ....*
    FROM *tableName*
    WHERE *condition*;

 - optionally instead of listing columns can use * to select all columns from table
 - where condition accepts first parameter as column identifier and second parameter may be column identifer or string/number literal
 - where field not necessary
 - column and table name identifiers only accept strings that contain ASCII characters (a-zA-Z) as first character and (_a-zA-Z0-9) for second character (you can use the regex [_a-zA-Z][_a-zA-Z0-9]* to test if your string works), *'* is reserved for string literals, to specify and identifer literal use *"* and any sequence of characters within will be valid
 - max length of 255 bytes for column/table name

## Insert

Format:
    INSERT INTO *table_name* (*column1*, *column2*, *column3*, ...)
    VALUES (*value1*, *value2*, *value3*, ...);

 - must specify columns currently
 - values must be placed directly in sql string currently (will be changed to avoid sql injection in future)
 - same constraints for table/column name applies here
 - string literals use *'*, number literals can be integer or floats, true/false are reserved keywords for bool literals

## Create

Format:
    CREATE TABLE *table_name* (
    *column1* datatype constraint...,
    *column2* datatype constraint,
    *column3* datatype constraint,
    ....);

 - currently supported datatypes are INT, FLOAT, BOOL, CHAR. Where size of CHAR column must be specified (ie. column CHAR(number) constraint...)
 - same constraints for table/column name apply as above
 - total row size for all columns added together may not exceed 4070 bytes in total
 - CHAR field will error when trying to insert strings larger than specified but will allow strings lower in size
 - current constraints are nullable or not nullable and primary key
 - may only have one primary key and must be integer field