/*
 * Copyright 2025 Raghav Aggarwal
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.raghav;

import java.io.File;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class PrintDiagnosticsUDF extends GenericUDF {

  // Ensures the diagnostics are printed only once per JVM
  private static final AtomicBoolean hasPrinted = new AtomicBoolean(false);

  /**
   * 1. Called once at the beginning of the task. This is where you validate input types and declare
   * your return type.
   */
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    // This UDF accepts one argument of any type, so no specific checks are needed.
    if (arguments.length != 1) {
      throw new UDFArgumentException("This UDF requires exactly one argument.");
    }

    // This UDF will return a string. We must declare this upfront.
    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

  /** 2. Called for every row. This is the core logic. */
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    // The logic is moved inside the evaluate method and guarded by the AtomicBoolean.
    if (!hasPrinted.getAndSet(true)) {
      printDiagnostics();
    }

    // Return the original input value, which is in the first argument.
    if (arguments.length > 0 && arguments[0] != null) {
      return arguments[0].get();
    }
    return null;
  }

  /** 3. Helper method to keep the evaluate method clean. */
  private void printDiagnostics() {
    SessionState ss = SessionState.get();
    HiveConf conf = (ss == null) ? new HiveConf(PrintDiagnosticsUDF.class) : ss.getConf();

    // Print Classpath
    System.err.println("\n--- DUMPING TEZ TASK CLASSPATH (in order) ---");
    String classPath = System.getProperty("java.class.path");
    String separator = File.pathSeparator;
    for (String entry : classPath.split(separator)) {
      System.err.println(entry);
    }
    System.err.println("--- END OF CLASSPATH DUMP ---\n");

    // Print Configuration
    System.err.println("--- DUMPING TEZ TASK CONFIGURATION ---");
    for (Map.Entry<String, String> entry : conf) {
      System.err.println(entry.getKey() + "=" + entry.getValue());
    }
    System.err.println("--- END OF CONFIGURATION DUMP ---");
  }

  /** 4. Used for explaining the query plan. */
  @Override
  public String getDisplayString(String[] children) {
    return "print_diag(" + children[0] + ")";
  }
}
