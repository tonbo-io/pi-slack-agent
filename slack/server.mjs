import { createServer } from "node:http";
import { createConversations } from "./conversation.mjs";
import { SeenEvents } from "./dedupe.mjs";
import { createSlackRequestHandler } from "./http.mjs";
import { createLogger } from "./log.mjs";
import { createSlackClient } from "./slack-api.mjs";
import { createTonboClient } from "./tonbo.mjs";

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required.`);
  return value;
}

const log = createLogger();
const slack = createSlackClient({ botToken: required("SLACK_BOT_TOKEN") });
const tonbo = createTonboClient({
  origin: required("TONBO_AGENT_API_ORIGIN"),
  iamOrigin: required("TONBO_IAM_ORIGIN"),
  apiKey: required("TONBO_AGENT_API_KEY"),
  agentId: required("TONBO_AGENT_ID"),
});
const conversations = createConversations({
  tonbo,
  slack,
  agentId: required("TONBO_AGENT_ID"),
  teamId: required("SLACK_TEAM_ID"),
  botUserId: required("SLACK_BOT_USER_ID"),
  log,
});
const handler = createSlackRequestHandler({
  signingSecret: required("SLACK_SIGNING_SECRET"),
  teamId: required("SLACK_TEAM_ID"),
  seen: new SeenEvents(),
  onEvent: (event) => conversations.handle(event),
  log,
});
const port = Number(process.env.PORT || 9080);
const server = createServer((request, response) => {
  handler(request, response).catch((error) => {
    log("request_failed", { reason: error?.message });
    if (!response.headersSent) response.writeHead(500);
    response.end();
  });
});
server.listen(port, "0.0.0.0", () => log("slack_agent_listening", { port }));
for (const signal of ["SIGTERM", "SIGINT"])
  process.once(signal, () => {
    log("slack_agent_stopping", { signal, active: conversations.active });
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 5000).unref();
  });
