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

import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.genomics.utils.OfflineAuth;

import java.util.Scanner;

/**
 * Contains pipeline options relevant to the creation of Genomics API clients.
 */
public interface GenomicsOptions extends GcsOptions {

  public static class Methods {

    public static OfflineAuth getGenomicsAuth(GenomicsOptions options) {
      if (DirectPipelineRunner.class != options.getRunner()
          && null != options.getSecretsFile()
          && options.getWarnUserCredential()) {
        requestConfirmation("This pipeline will run on GCE VMs and your user credential will"
            + " be used by all Dataflow worker instances.  Your credentials may be visible to"
            + " others with access to the VMs.");
      }

      if (null != options.getSecretsFile()) {
        // User credential will be available on all Dataflow workers.
        return new OfflineAuth(options.getGcpCredential());
      }
      // This "empty" OfflineAuth will default to the Application
      // Default Credential available from whereever it is
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

  @Description("Set this option to 'false' to disable the yes/no prompt when running"
      + " the pipeline with a user credential.")
  @Default.Boolean(true)
  boolean getWarnUserCredential();

  void setWarnUserCredential(boolean warnUserCredential);

  @Description("Specifies number of results to return in a single page of results. "
      + "If unspecified, the default page size for the Genomics API is used.")
  @Default.Integer(0)
  int getPageSize();

  void setPageSize(int pageSize);
}
