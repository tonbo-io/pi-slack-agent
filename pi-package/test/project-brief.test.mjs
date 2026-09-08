import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import registerProjectBrief from "../extensions/project-brief.js";

test("the Agent's custom tool reads the actual deployed project brief", async () => {
  let tool;
  registerProjectBrief({
    registerTool: (registered) => {
      tool = registered;
    },
  });
  assert.equal(tool.name, "read_project_brief");
  const result = await tool.execute();
  assert.deepEqual(result.content, [
    { type: "text", text: readFileSync(new URL("../project-brief.md", import.meta.url), "utf8") },
  ]);
});
