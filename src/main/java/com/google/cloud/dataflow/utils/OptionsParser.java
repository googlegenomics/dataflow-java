// TODO: Stop copying this file from dataflow and use a maven dependency
/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.utils;

import com.google.cloud.dataflow.sdk.runners.Description;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.annotation.Nullable;

/**
 * A utility which uses Apache Commons CLI command-line interface to parse
 * options annotated with {@code @OptionInfo}.
 */
public class OptionsParser {

  private static final Logger LOG = Logger.getLogger(
      OptionsParser.class.getName());

  private Options options;

  /**
   * Reads options from a class specified by {@code type}.
   * <p>
   * All fields within the options class that are annotated with
   * {@link Description} are turned into command-line arguments.
   * <p>
   * If a field is annotated with {@link RequiredOption} and no value is
   * provided, either by the command-line or from defaults, then an error is
   * displayed and an exception is thrown.
   * <p>
   * If the command line contains {@code --help}, then the list of commands is
   * displayed and the program exits.
   *
   * @param args command-line arguments
   * @param type type of options class to create
   * @param programName name of the program, used for help messages, or null
   * @param <T> type of options class to create
   * @return an instance of the options class
   */
  public static <T> T parse(String[] args, Class<T> type,
      @Nullable String programName) {
    if (programName == null) {
      StackTraceElement[] stack = Thread.currentThread().getStackTrace();
      try {
        programName = Class.forName(stack[stack.length - 1].getClassName())
            .getSimpleName();
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException(
            "Couldn't find main class.  This should never happen.");
      }
    }

    OptionsParser parser = new OptionsParser();

    Map<String, Class<?>> fieldTypes = new HashMap<>();
    fieldTypes.put("help", Boolean.TYPE);
    Map<String, String> defaultValues = new HashMap<>();
    defaultValues.put("appName", programName);

    List<String> requiredArguments = new LinkedList<>();
    for (Field f : type.getFields()) {
      Description description = f.getAnnotation(Description.class);
      if (description == null) {
        continue;
      }

      Option option =
          new Option(f.getName(), f.getName(), true, description.value());
      if (f.getType().equals(Boolean.TYPE)) {
        // Booleans may be specified without an argument.
        option.setOptionalArg(true);
      }
      parser.addOption(option);

      if (f.getAnnotation(RequiredOption.class) != null) {
        requiredArguments.add(f.getName());
      }
      fieldTypes.put(f.getName(), f.getType());
    }

    try {
      ObjectNode properties = parser.parseArgs(args, fieldTypes);

      if (properties.get("help") != null) {
        parser.printHelp(programName);
        System.exit(1);
      }

      for (Map.Entry<String, String> entry : defaultValues.entrySet()) {
        if (!properties.has(entry.getKey())) {
          properties.put(entry.getKey(), entry.getValue());
        }
      }

      boolean ok = true;
      for (String argName : requiredArguments) {
        if (properties.get(argName) == null) {
          System.err.println("Missing required argument: " + argName);
          ok = false;
        }
      }
      if (!ok) {
        parser.printHelp(programName);
        throw new IllegalArgumentException("Missing required arguments: " +
            requiredArguments);
      }

      ObjectMapper mapper = new ObjectMapper();
      // Ignore properties which are not used by the object.
      mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

      LOG.fine("Provided: " +
          mapper.writerWithDefaultPrettyPrinter().writeValueAsString(properties));

      T result = mapper.treeToValue(properties, type);
      LOG.fine("Final Options: " +
          mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
      return result;
    } catch (ParseException | IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Reads options from {@link PipelineOptions}.
   * <p>
   * All fields within the {@link PipelineOptions} class that are annotated with
   * {@link Description} are turned into command-line arguments.
   * <p>
   * If a field is annotated with {@link RequiredOption} and no value is
   * provided, either by the command-line or from defaults, then an error is
   * displayed and an exception is thrown.
   * <p>
   * If the command line contains {@code --help}, then the list of commands is
   * displayed and the program exits.
   *
   * @param args command-line arguments
   */
  public static PipelineOptions parse(String[] args) {
    return parse(args, PipelineOptions.class, null);
  }

  /**
   * Build the handler.
   */
  private OptionsParser() {
    options = new Options();
    options.addOption(new Option(
        "help",
        "help",
        false,
        "Print out a help message."));
  }

  private OptionsParser addOption(Option option) {
    options.addOption(option);
    return this;
  }

  private ObjectNode parseArgs(String[] args, Map<String, Class<?>> types)
      throws ParseException {
    CommandLineParser parser = new GnuParser();
    CommandLine line = parser.parse(options, args);

    ObjectNode props = new ObjectNode(JsonNodeFactory.instance);

    for (Object o : options.getOptions()) {
      Option option = (Option) o;
      String optionName = option.getLongOpt();
      if (line.hasOption(optionName)) {
        Class<?> fieldType = types.get(optionName);
        String optionValue = option.hasArg() ?
                             line.getOptionValue(optionName) : null;

        if (optionValue != null) {
          if (fieldType.isArray() ||
              Collection.class.isAssignableFrom(fieldType)) {
            ArrayNode arrayNode = new ArrayNode(JsonNodeFactory.instance);
            for (String s : optionValue.split(",")) {
              arrayNode.add(s);
            }
            props.put(optionName, arrayNode);
          } else {
            props.put(optionName, optionValue);
          }
        } else if (fieldType.equals(Boolean.TYPE)) {
          // Treat it as a boolean flag that is true when supplied.
          props.put(optionName, true);
        }
      }
    }
    return props;
  }

  /**
   * Print a help message.
   */
  private void printHelp(String programName) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(programName + " [options]", options);
  }
}
