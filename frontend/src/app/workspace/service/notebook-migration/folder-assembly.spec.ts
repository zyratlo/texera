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

import {
  buildFolderDocument,
  checkFolderLimits,
  compareFolderPaths,
  fileBanner,
  isMigratablePythonPath,
  MAX_FOLDER_CHARACTERS,
  MAX_FOLDER_FILES,
} from "./folder-assembly";
import { segmentScript } from "./script-segmentation";

describe("isMigratablePythonPath", () => {
  it("accepts Python source at any depth, whatever the extension's case", () => {
    expect(isMigratablePythonPath("train.py")).toBe(true);
    expect(isMigratablePythonPath("pkg/models/train.py")).toBe(true);
    expect(isMigratablePythonPath("pkg/Train.PY")).toBe(true);
  });

  it("rejects anything that is not a .py file", () => {
    expect(isMigratablePythonPath("notes.ipynb")).toBe(false);
    expect(isMigratablePythonPath("requirements.txt")).toBe(false);
    expect(isMigratablePythonPath("pkg/py")).toBe(false);
    expect(isMigratablePythonPath("")).toBe(false);
  });

  it("rejects hidden files and anything under a hidden directory", () => {
    expect(isMigratablePythonPath(".hidden.py")).toBe(false);
    expect(isMigratablePythonPath(".venv/lib/thing.py")).toBe(false);
    expect(isMigratablePythonPath("pkg/.tox/run.py")).toBe(false);
  });

  it("rejects caches and vendored dependencies wherever they sit in the tree", () => {
    expect(isMigratablePythonPath("__pycache__/train.py")).toBe(false);
    expect(isMigratablePythonPath("pkg/__pycache__/train.py")).toBe(false);
    expect(isMigratablePythonPath("venv/lib/site-packages/numpy/core.py")).toBe(false);
    expect(isMigratablePythonPath("a/node_modules/b/x.py")).toBe(false);
    expect(isMigratablePythonPath("build/lib/train.py")).toBe(false);
    expect(isMigratablePythonPath("dist/train.py")).toBe(false);
  });

  it("keeps test files, which are logic the user may want represented", () => {
    expect(isMigratablePythonPath("tests/test_train.py")).toBe(true);
  });
});

describe("compareFolderPaths", () => {
  function sorted(paths: string[]): string[] {
    return [...paths].sort(compareFolderPaths);
  }

  it("groups by directory before comparing names", () => {
    expect(sorted(["b/one.py", "a/two.py", "a/one.py"])).toEqual(["a/one.py", "a/two.py", "b/one.py"]);
  });

  it("puts a package initializer ahead of its siblings", () => {
    expect(sorted(["pkg/aaa.py", "pkg/__init__.py"])).toEqual(["pkg/__init__.py", "pkg/aaa.py"]);
  });

  it("orders root-level files before files in a subdirectory", () => {
    expect(sorted(["pkg/train.py", "main.py"])).toEqual(["main.py", "pkg/train.py"]);
  });

  it("reports equality for the same path", () => {
    expect(compareFolderPaths("a/x.py", "a/x.py")).toBe(0);
  });

  it("keeps a directory adjacent to its own subdirectories", () => {
    // "-" sorts before "/", so comparing directory paths as plain strings would wedge src-old
    // between src and src/utils, and the layout would then nest src/utils under src-old.
    expect(sorted(["src/a.py", "src-old/b.py", "src/utils/c.py"])).toEqual([
      "src/a.py",
      "src/utils/c.py",
      "src-old/b.py",
    ]);
  });

  it("orders nested directories by segment, not by raw string", () => {
    expect(sorted(["a.b/x.py", "a/y.py", "a/b/z.py"])).toEqual(["a/y.py", "a/b/z.py", "a.b/x.py"]);
  });
});

describe("buildFolderDocument", () => {
  const files = [
    { path: "pkg/train.py", source: "train1\ntrain2" },
    { path: "main.py", source: "main1" },
  ];

  it("concatenates files in sorted order, each behind its banner", () => {
    const document = buildFolderDocument(files);

    expect(document.source.split("\n")).toEqual([
      fileBanner("main.py"),
      "main1",
      fileBanner("pkg/train.py"),
      "train1",
      "train2",
    ]);
  });

  it("reports each file's span and its banner as a forced boundary", () => {
    const document = buildFolderDocument(files);

    expect(document.files).toEqual([
      { path: "main.py", startLine: 1, endLine: 2 },
      { path: "pkg/train.py", startLine: 3, endLine: 5 },
    ]);
    expect(document.forcedBoundaries).toEqual([1, 3]);
  });

  it("drops files holding nothing but whitespace, so no cell is a lone banner", () => {
    const document = buildFolderDocument([
      { path: "a.py", source: "a1" },
      { path: "b.py", source: "   \n\n" },
    ]);

    expect(document.files.map(file => file.path)).toEqual(["a.py"]);
    expect(document.source).toBe(`${fileBanner("a.py")}\na1`);
  });

  it("normalizes CRLF and does not count a trailing newline as a line", () => {
    const document = buildFolderDocument([{ path: "a.py", source: "a1\r\na2\r\n" }]);

    expect(document.source).toBe(`${fileBanner("a.py")}\na1\na2`);
    expect(document.files[0]).toEqual({ path: "a.py", startLine: 1, endLine: 3 });
  });

  it("returns an empty document for an empty selection", () => {
    expect(buildFolderDocument([])).toEqual({ source: "", forcedBoundaries: [], files: [] });
  });

  it("leaves the caller's array untouched", () => {
    const input = [...files];
    buildFolderDocument(input);

    expect(input.map(file => file.path)).toEqual(["pkg/train.py", "main.py"]);
  });
});

describe("buildFolderDocument with segmentScript", () => {
  // Sequential ids keep assertions readable; the real factory is uuidv4.
  function counterIds(): () => string {
    let next = 0;
    return () => `c${++next}`;
  }

  it("keeps every cell inside one file even when a reported range spans two", () => {
    const document = buildFolderDocument([
      { path: "a.py", source: "a1\na2" },
      { path: "b.py", source: "b1\nb2" },
    ]);

    // a.py occupies lines 1-3 and b.py lines 4-6; one UDF claims across the seam.
    const { cells } = segmentScript(
      document.source,
      { UDF1: [[2, 5]] },
      { forcedBoundaries: document.forcedBoundaries, newUuid: counterIds() }
    );

    // The cut at line 4 is the banner of b.py, so no cell holds lines from both files.
    expect(cells.map(cell => [cell.startLine, cell.endLine])).toEqual([
      [1, 1],
      [2, 3],
      [4, 5],
      [6, 6],
    ]);
  });

  it("still maps a straddling range to every cell it covers", () => {
    const document = buildFolderDocument([
      { path: "a.py", source: "a1\na2" },
      { path: "b.py", source: "b1\nb2" },
    ]);

    const { udfToCellUuids } = segmentScript(
      document.source,
      { UDF1: [[2, 5]] },
      { forcedBoundaries: document.forcedBoundaries, newUuid: counterIds() }
    );

    expect(udfToCellUuids["UDF1"]).toEqual(["c2", "c3"]);
  });
});

describe("checkFolderLimits", () => {
  it("passes a folder within both caps", () => {
    expect(checkFolderLimits(3, 1000)).toBeNull();
    expect(checkFolderLimits(MAX_FOLDER_FILES, MAX_FOLDER_CHARACTERS)).toBeNull();
  });

  it("reports a selection with no Python files", () => {
    expect(checkFolderLimits(0, 0)).toContain("No Python files");
  });

  it("reports the counts that exceeded each cap", () => {
    expect(checkFolderLimits(MAX_FOLDER_FILES + 1, 10)).toContain((MAX_FOLDER_FILES + 1).toLocaleString());
    expect(checkFolderLimits(1, MAX_FOLDER_CHARACTERS + 1)).toContain((MAX_FOLDER_CHARACTERS + 1).toLocaleString());
  });
});
