/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.genomics.dataflow.utils;

import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import com.google.cloud.genomics.utils.OfflineAuth;

import java.util.Scanner;

/**
 * Contains pipeline options relevant to the creation of Genomics API clients.
 */

// TODO: This class has almost nothing in it now. Refactor it out of the codebase.
public interface GenomicsOptions extends GcsOptions {

  public static class Methods {

    public static OfflineAuth getGenomicsAuth(GenomicsOptions options) {
      // This "empty" OfflineAuth will default to the Application
      // Default Credential available from where ever it is
      // accessed (e.g., locally or on GCE).
      return new OfflineAuth();
    }

    /**
     * Print the passed message and ask the user whether to proceed.  If
     * the user's response is not affirmative, exit.
     *
     * @param message The message requiring confirmation.
     */
    public static void requestConfirmation(final String message) {
      System.out.println("\n" + message);
      System.out.println("Do you want to continue (Y/n)?");
      Scanner kbd = new Scanner(System.in);
      String decision;
      decision = kbd.nextLine();
      kbd.close();
      switch(decision) {
        case "yes": case "Yes": case "YES": case "y": case "Y":
          break;
        default:
          System.exit(0);
      }
    }
  }
}
