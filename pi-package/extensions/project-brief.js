import { readFileSync } from "node:fs";

export default function registerProjectBrief(pi) {
  pi.registerTool({
    name: "read_project_brief",
    label: "Read project brief",
    description: "Read this Agent's project brief from its deployed repository.",
    parameters: { type: "object", properties: {}, additionalProperties: false },
    execute: async () => ({
      content: [
        {
          type: "text",
          text: readFileSync(new URL("../project-brief.md", import.meta.url), "utf8"),
        },
      ],
      details: {},
    }),
  });
}
