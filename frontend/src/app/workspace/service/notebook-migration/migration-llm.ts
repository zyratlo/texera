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
import { createOpenAI } from "@ai-sdk/openai";
import { generateText, type ModelMessage } from "ai";
import { AppSettings } from "../../../common/app-setting";
import { v4 as uuidv4 } from "uuid";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { OperatorPredicate } from "../../types/workflow-common.interface";
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
  operators: OperatorPredicate[];
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

/**
 * Wraps a single LLM chat session that converts a Jupyter notebook into a Texera
 * workflow plus a cell<->operator mapping.
 *
 * Lifecycle: `initialize()` -> `verifyConnection()` (optional) ->
 * `convertNotebookToWorkflow()` -> `close()`. The session keeps a running `messages`
 * history shared by the prompts within one conversion. `convertNotebookToWorkflow()`
 * resets that history to the documentation prelude at its start, so the same instance
 * can convert multiple notebooks without leaking one conversion's context into the next.
 *
 * Output column types: intermediate UDFs declare their output columns as `binary` so rich
 * Python objects (DataFrames, arrays, models) round-trip between operators via pickle.
 * Terminal UDFs (no outgoing edge) declare their outputs as `string` so the result panel
 * renders viewable values rather than opaque binary blobs.
 */
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

  constructor(
    private config: GuiConfigService,
    private workflowUtilService: WorkflowUtilService
  ) {}

  private get enabled(): boolean {
    return this.config.env.pythonNotebookMigrationEnabled;
  }

  private assertEnabled(): void {
    if (!this.enabled) {
      throw new Error("Notebook migration feature is disabled");
    }
  }

  /**
   * Seed the conversation with the Texera documentation prelude, discarding any
   * prior conversation. Used by initialize() and at the start of each conversion.
   */
  private seedDocumentation(): void {
    this.messages = NotebookMigrationLLM.DOCUMENTATION.map(
      (doc): ModelMessage => ({
        role: "system",
        content: doc,
      })
    );
  }

  private parseJsonResponse(raw: string, context: string): any {
    // Trim first, then strip optional markdown code fences (```json ... ``` or ``` ... ```)
    const cleaned = raw
      .trim()
      .replace(/^```[a-zA-Z]*\s*/, "")
      .replace(/\s*```$/, "")
      .trim();
    try {
      return JSON.parse(cleaned);
    } catch (err) {
      throw new Error(`Failed to parse LLM ${context} response as JSON: ${(err as Error).message}`);
    }
  }

  /**
   * Initialize a new LLM session with Texera documentation
   */
  public initialize(modelType: string = "gpt-5-mini", apiKey: string = "dummy"): void {
    this.assertEnabled();
    this.model = createOpenAI({
      baseURL: new URL(`${AppSettings.getApiEndpoint()}`, document.baseURI).toString(),
      // apiKey is required by the library for creating the OpenAI compatible client;
      // For security reason, we store the apiKey at the backend, thus the value is dummy here.
      apiKey: apiKey,
    }).chat(modelType);

    this.seedDocumentation();

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
  private async sendPrompt(prompt: string): Promise<string> {
    if (!this.initialized) {
      throw new Error("LLM session not initialized");
    }

    this.messages.push({
      role: "user",
      content: prompt,
    });

    const result = await generateText({
      model: this.model,
      messages: this.messages,
    });

    this.messages.push({
      role: "assistant",
      content: result.text,
    });

    return result.text;
  }

  /**
   * Send a Jupyter Notebook to be converted into a workflow and mapping.
   */
  public async convertNotebookToWorkflow(notebook: Notebook): Promise<string> {
    this.assertEnabled();
    if (!this.initialized) {
      throw new Error("LLM session not initialized");
    }

    // Reset to the documentation prelude so a prior conversion's prompts/responses
    // don't leak into this one. The two sendPrompt calls below still share history.
    this.seedDocumentation();

    const codeCells = notebook.cells.filter(cell => cell.cell_type === "code");
    const notebookString = codeCells
      .map(cell => {
        const uuid = String(cell.metadata.uuid);
        return `# START ${uuid}\n${cell.source}\n# END ${uuid}`;
      })
      .join("\n\n");

    const workflow = await this.sendPrompt(`${WORKFLOW_PROMPT}\n${notebookString}`);
    const mapping = await this.sendPrompt(MAPPING_PROMPT);

    // Remove ```json blocks and parse
    const udfLLMResponse = this.parseJsonResponse(workflow, "workflow");

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

    // UDFs that are never the source of an edge are terminal (result-facing). Their outputs
    // default to "string" so the result panel renders typed values; intermediate UDFs keep
    // "binary" so rich objects (DataFrames, arrays, models) round-trip between operators via pickle.
    const edgeSources = new Set<string>((udfLLMResponse.edges || []).map(([source]: [string, string]) => source));

    Object.entries(udfLLMResponse.code).forEach(([udfId, udfCode], i) => {
      let udfOutputColumns: { attributeName: string; attributeType: string }[] = [];
      if (udfLLMResponse.outputs && udfLLMResponse.outputs[udfId]) {
        const attributeType = edgeSources.has(udfId) ? "binary" : "string";
        udfOutputColumns = udfLLMResponse.outputs[udfId].map((attr: string) => ({
          attributeName: attr,
          attributeType,
        }));
      }

      // Build the operator from the live PythonUDFV2 schema so the operatorVersion, ports, and
      // property defaults track the backend definition, then overlay the generated code/outputs.
      const base = this.workflowUtilService.getNewOperatorPredicate("PythonUDFV2", udfId);
      const operator: OperatorPredicate = {
        ...base,
        operatorProperties: {
          ...base.operatorProperties,
          code: udfCode,
          retainInputColumns: false,
          outputColumns: udfOutputColumns,
        },
      };

      udfMappingToUUID[udfId] = operator.operatorID;
      workflowJSON.operators.push(operator);
      workflowJSON.operatorPositions[operator.operatorID] = { x: 140 * (i + 1), y: 0 };
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
    const parsedMapping: Record<string, string[]> = this.parseJsonResponse(mapping, "mapping");

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

    return JSON.stringify({ workflowJSON, workflowNotebookMapping });
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
