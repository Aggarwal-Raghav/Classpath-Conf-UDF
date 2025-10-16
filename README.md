## Steps to run:

```
ADD JAR hdfs:///path/to/your/print-diag-udf.jar;
CREATE TEMPORARY FUNCTION print_diag AS 'com.github.raghav.PrintDiagnosticsUDF';

SELECT
  t.some_column,
  print_diag(t.some_column)
FROM
  your_table t
LIMIT 5;

```

