/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Injectable } from "@angular/core";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { firstValueFrom, from, Observable, of } from "rxjs";
import { map } from "rxjs/operators";
import { createOpenAI } from "@ai-sdk/openai";
import { generateText, type ModelMessage } from "ai";
import { AppSettings } from "../../../common/app-setting";
import { v4 as uuidv4 } from "uuid";
import {
  TEXERA_OVERVIEW,
  TUPLE_DOCUMENTATION,
  TABLE_DOCUMENTATION,
  OPERATOR_DOCUMENTATION,
  UDF_INPUT_PORT_DOCUMENTATION,
  EXAMPLE_OF_GOOD_CONVERSION,
  VISUALIZER_DOCUMENTATION,
  EXAMPLE_OF_MULTIPLE_UDF_CONVERSION,
  WORKFLOW_PROMPT,
  MAPPING_PROMPT,
} from "./migration-prompts";

interface Cell {
  cell_type: string;
  metadata: { [key: string]: any };
  source: string;
}

export interface Notebook {
  cells: Cell[];
}

interface WorkflowJSON {
  operators: any[];
  operatorPositions: Record<string, { x: number; y: number }>;
  links: any[];
  commentBoxes: any[];
  settings: {
    dataTransferBatchSize: number;
  };
}

interface CombinedMapping {
  operator_to_cell: Record<string, string[]>;
  cell_to_operator: Record<string, string[]>;
}

@Injectable()
export class NotebookMigrationLLM {
  private model: any;
  private messages: ModelMessage[] = [];
  private initialized = false;

  private static readonly DOCUMENTATION: string[] = [
    TEXERA_OVERVIEW,
    TUPLE_DOCUMENTATION,
    TABLE_DOCUMENTATION,
    OPERATOR_DOCUMENTATION,
    EXAMPLE_OF_GOOD_CONVERSION,
    VISUALIZER_DOCUMENTATION,
    UDF_INPUT_PORT_DOCUMENTATION,
    EXAMPLE_OF_MULTIPLE_UDF_CONVERSION,
  ];

  constructor(private config: GuiConfigService) {}

  private get enabled(): boolean {
    return this.config.env.pythonNotebookMigrationEnabled;
  }

  /**
   * Initialize a new LLM session with Texera documentation
   */
  public initialize(modelType: string = "gpt-5-mini", apiKey: string = "dummy"): void {
    if (!this.enabled) return;
    this.model = createOpenAI({
      baseURL: new URL(`${AppSettings.getApiEndpoint()}`, document.baseURI).toString(),
      // apiKey is required by the library for creating the OpenAI compatible client;
      // For security reason, we store the apiKey at the backend, thus the value is dummy here.
      apiKey: apiKey,
    }).chat(modelType);

    this.messages = [
      ...NotebookMigrationLLM.DOCUMENTATION.map(
        (doc): ModelMessage => ({
          role: "system",
          content: doc,
        })
      ),
    ];

    this.initialized = true;
  }

  /**
   * Verify the connection to the LLM using the given API key
   */
  public async verifyConnection(): Promise<boolean> {
    if (!this.enabled) return false;
    if (!this.initialized) {
      throw new Error("LLM session not initialized");
    }

    try {
      await generateText({
        model: this.model,
        messages: [
          {
            role: "user",
            content: "ping",
          },
        ],
        maxOutputTokens: 10,
      });

      return true;
    } catch (err) {
      console.error("API key verification failed:", err);
      return false;
    }
  }

  /**
   * Send a prompt and receive a response.
   * All prior documentation and conversation is preserved.
   */
  private sendPrompt(prompt: string): Observable<string> {
    if (!this.initialized) {
      throw new Error("LLM session not initialized");
    }

    this.messages.push({
      role: "user",
      content: prompt,
    });

    return from(
      generateText({
        model: this.model,
        messages: this.messages,
      })
    ).pipe(
      map(result => {
        this.messages.push({
          role: "assistant",
          content: result.text,
        });

        return result.text;
      })
    );
  }

  /**
   * Send a Jupyter Notebook to be converted into a workflow and mapping.
   */
  public async convertNotebookToWorkflow(notebook: Notebook): Promise<Observable<string>> {
    if (!this.enabled) throw new Error("Notebook migration feature is disabled");
    if (!this.initialized) {
      throw new Error("LLM session not initialized");
    }

    const codeCells = notebook.cells.filter(cell => cell.cell_type === "code");
    const notebookString = codeCells
      .map(cell => {
        const uuid = String(cell.metadata.uuid);
        return `# START ${uuid}\n${cell.source}\n# END ${uuid}`;
      })
      .join("\n\n");

    const workflow = await firstValueFrom(this.sendPrompt(`${WORKFLOW_PROMPT}\n${notebookString}`));
    const mapping = await firstValueFrom(this.sendPrompt(MAPPING_PROMPT));

    // Remove ```json blocks and parse
    const udfLLMResponse = JSON.parse(workflow.replace(/^```json\s*|```$/g, "").trim());

    const workflowJSON: WorkflowJSON = {
      operators: [],
      operatorPositions: {},
      links: [],
      commentBoxes: [],
      settings: {
        dataTransferBatchSize: 400,
      },
    };

    const udfMappingToUUID: Record<string, string> = {};

    Object.entries(udfLLMResponse.code).forEach(([udfId, udfCode], i) => {
      const udfUUID = `PythonUDFV2-operator-${uuidv4()}`;
      udfMappingToUUID[udfId] = udfUUID;

      let udfOutputColumns: { attributeName: string; attributeType: string }[] = [];
      if (udfLLMResponse.outputs && udfLLMResponse.outputs[udfId]) {
        udfOutputColumns = udfLLMResponse.outputs[udfId].map((attr: string) => ({
          attributeName: attr,
          attributeType: "binary",
        }));
      }

      // Add UDF to operators
      workflowJSON.operators.push({
        operatorID: udfUUID,
        operatorType: "PythonUDFV2",
        operatorVersion: "3d69fdcedbb409b47162c4b55406c77e54abe416",
        operatorProperties: {
          code: udfCode,
          workers: 1,
          retainInputColumns: false,
          outputColumns: udfOutputColumns,
        },
        inputPorts: [
          {
            portID: "input-0",
            displayName: "",
            allowMultiInputs: true,
            isDynamicPort: false,
            dependencies: [],
          },
        ],
        outputPorts: [
          {
            portID: "output-0",
            displayName: "",
            allowMultiInputs: false,
            isDynamicPort: false,
          },
        ],
        showAdvanced: false,
        isDisabled: false,
        customDisplayName: udfId,
        dynamicInputPorts: true,
        dynamicOutputPorts: true,
      });

      // Add UDF to operatorPositions
      workflowJSON.operatorPositions[udfUUID] = { x: 140 * (i + 1), y: 0 };
    });

    // Add links/edges
    (udfLLMResponse.edges || []).forEach(([source, target]: [string, string]) => {
      workflowJSON.links.push({
        linkID: `link-${uuidv4()}`,
        source: {
          operatorID: udfMappingToUUID[source],
          portID: "output-0",
        },
        target: {
          operatorID: udfMappingToUUID[target],
          portID: "input-0",
        },
      });
    });

    // Parse mapping
    const parsedMapping: Record<string, string[]> = JSON.parse(mapping.replace(/^```json\s*|```$/g, "").trim());

    const udfToCell: Record<string, string[]> = {};
    const cellToUdf: Record<string, string[]> = {};

    Object.entries(parsedMapping).forEach(([udf, cells]) => {
      const udfUUID = udfMappingToUUID[udf];
      udfToCell[udfUUID] = cells;
      cells.forEach(cell => {
        if (!cellToUdf[cell]) {
          cellToUdf[cell] = [udfUUID];
        } else {
          cellToUdf[cell].push(udfUUID);
        }
      });
    });

    const workflowNotebookMapping: CombinedMapping = {
      operator_to_cell: udfToCell,
      cell_to_operator: cellToUdf,
    };

    const result = JSON.stringify({ workflowJSON, workflowNotebookMapping });
    return of(result);
  }

  /**
   * Closes the session.
   * Clears all context and releases references.
   */
  public close(): void {
    this.messages = [];
    this.model = null;
    this.initialized = false;
  }
}
