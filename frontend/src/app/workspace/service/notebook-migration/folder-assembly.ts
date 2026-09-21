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

import { splitScriptLines } from "./script-segmentation";

/**
 * Assembles a folder of Python sources into the one numbered document the migration LLM sees.
 *
 * A folder is converted as a single input rather than as N inputs: the model reports line ranges
 * over one concatenated document, exactly as it does for a single script, so the mapping stays
 * keyed on cell uuids and nothing downstream (storage, the Jupyter panel, highlighting) learns
 * that folders exist. Provenance is carried in the document itself, by a banner line before each
 * file that becomes the first line of that file's first cell.
 *
 * Lines are split with the segmenter's own splitScriptLines, which is also what numbers the
 * prompt and slices the cells, so assembly, numbering and slicing agree on what a line is.
 */

// Path segments that never hold code a user means to migrate. Matched per segment, so a cache or
// a vendored dependency is excluded wherever in the tree it sits. Hidden segments are handled
// separately by the leading-dot rule, which already covers .venv, .git and friends.
const EXCLUDED_SEGMENTS = new Set(["__pycache__", "venv", "site-packages", "node_modules", "build", "dist"]);

/**
 * Caps on one conversion, enforced before any request is sent.
 *
 * The binding constraint is the reply, not the prompt. The conversion asks the model to return
 * every line of the folder's code, as JSON-escaped strings, so the reply has to be at least as
 * large as the input; and no output budget is set, so each model uses whatever ceiling it has.
 * Input context is by far the roomier side, which is why this sits well below what a context
 * window alone would allow. A cap the model cannot meet would defeat the point of refusing up
 * front: the user would wait out a full conversion and get a JSON parse error from a truncated
 * reply. Raise it once a larger folder has been measured converting end to end.
 */
export const MAX_FOLDER_FILES = 100;
export const MAX_FOLDER_CHARACTERS = 60_000;

export const FILE_BANNER_PREFIX = "# ===== FILE: ";
export const FILE_BANNER_SUFFIX = " =====";

/** The line that introduces a file in the assembled document. */
export function fileBanner(path: string): string {
  return `${FILE_BANNER_PREFIX}${path}${FILE_BANNER_SUFFIX}`;
}

export interface FolderFile {
  // Path relative to the selected folder, "/" separated, as webkitRelativePath reports it.
  path: string;
  source: string;
}

export interface FolderFileSpan {
  path: string;
  // 1-indexed and inclusive, spanning the banner line through the file's last line.
  startLine: number;
  endLine: number;
}

export interface FolderDocument {
  // The concatenated source, which is what gets numbered for the prompt and segmented afterwards.
  source: string;
  // Each file's banner line, passed to segmentScript so no cell straddles a file.
  forcedBoundaries: number[];
  // The files that made it into the document, in the order they appear in it.
  files: FolderFileSpan[];
}

/** True for a path that holds project Python source, as opposed to a cache, a vendored dependency
 * or a hidden file. Tests are kept: a test file is real logic the user may want represented, and
 * dropping it silently is the failure the mapping exists to prevent. */
export function isMigratablePythonPath(path: string): boolean {
  const segments = path.split("/").filter(segment => segment !== "" && segment !== ".");
  if (segments.length === 0) return false;
  if (!segments[segments.length - 1].toLowerCase().endsWith(".py")) return false;
  return !segments.some(segment => segment.startsWith(".") || EXCLUDED_SEGMENTS.has(segment));
}

function pathParts(path: string): { dir: string; name: string } {
  const index = path.lastIndexOf("/");
  return index === -1 ? { dir: "", name: path } : { dir: path.slice(0, index), name: path.slice(index + 1) };
}

/**
 * Sort key that makes a directory sort immediately before its own subdirectories.
 *
 * Comparing directory paths as plain strings gets this wrong, because "/" (0x2F) sorts after
 * characters that are legal in a name, such as "-" and ".". That puts "src-old" between "src"
 * and "src/utils", and the tree then renders src/utils nested under src-old. Swapping the
 * separator for NUL, which is below every printable character, restores segment-by-segment order.
 */
function directorySortKey(directory: string): string {
  return directory.split("/").join("\u0000");
}

/**
 * Orders files by directory, then by name with a package's `__init__.py` ahead of its siblings.
 *
 * Deterministic and explainable, which is the point: the model infers dataflow from the code
 * itself, so a wrong guess at import order would cost more than a stable one. Plain comparison
 * rather than localeCompare so the order does not shift with the browser's locale.
 */
export function compareFolderPaths(a: string, b: string): number {
  const left = pathParts(a);
  const right = pathParts(b);
  if (left.dir !== right.dir) {
    const leftKey = directorySortKey(left.dir);
    const rightKey = directorySortKey(right.dir);
    return leftKey < rightKey ? -1 : 1;
  }
  const leftIsInit = left.name === "__init__.py";
  const rightIsInit = right.name === "__init__.py";
  if (leftIsInit !== rightIsInit) return leftIsInit ? -1 : 1;
  if (left.name === right.name) return 0;
  return left.name < right.name ? -1 : 1;
}

/**
 * Concatenates the selected files into one document, banner line first for each.
 *
 * Files holding nothing but whitespace are dropped: they would contribute a banner and no code,
 * which renders as a cell containing only a heading. `files` reports what was actually included,
 * so a caller counting files counts the same set the model saw.
 */
export function buildFolderDocument(files: readonly FolderFile[]): FolderDocument {
  const ordered = [...files].sort((a, b) => compareFolderPaths(a.path, b.path));
  const documentLines: string[] = [];
  const spans: FolderFileSpan[] = [];

  for (const file of ordered) {
    const lines = splitScriptLines(file.source);
    if (lines.every(line => line.trim() === "")) continue;
    const startLine = documentLines.length + 1;
    documentLines.push(fileBanner(file.path), ...lines);
    spans.push({ path: file.path, startLine, endLine: documentLines.length });
  }

  return {
    source: documentLines.join("\n"),
    forcedBoundaries: spans.map(span => span.startLine),
    files: spans,
  };
}

/**
 * Checks a selection against the caps. Returns the message to show the user, or null when the
 * folder is within limits.
 *
 * Refusing up front costs the user nothing and says what to do about it. Truncating to fit would
 * instead produce a silently incomplete workflow, which is exactly what the mapping exists to
 * let users catch.
 */
export function checkFolderLimits(fileCount: number, characterCount: number): string | null {
  if (fileCount === 0) {
    return "No Python files were found in the selected folder.";
  }
  if (fileCount > MAX_FOLDER_FILES) {
    return `The selected folder has ${fileCount.toLocaleString()} Python files, more than the ${MAX_FOLDER_FILES.toLocaleString()} this tool converts at once. Select a smaller folder.`;
  }
  if (characterCount > MAX_FOLDER_CHARACTERS) {
    // Counts the assembled document, so it includes the banner line added per file.
    return `The selected folder's Python code totals ${characterCount.toLocaleString()} characters, more than the ${MAX_FOLDER_CHARACTERS.toLocaleString()} this tool converts at once. Select a smaller folder.`;
  }
  return null;
}
