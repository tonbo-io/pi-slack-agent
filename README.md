# Pi Slack Agent

A Pi Agent with a small, editable project brief, a custom tool that reads it, and the Slack application that connects the two. The `slack/` directory is the Agent's application service: it receives Slack events at the Agent's own hostname, verifies them, runs a Turn on this Agent through Tonbo's Management API, and streams the reply back into the Slack thread while it is produced. Slack app credentials are delivered to the service as application secrets; they are never committed here.

## Use in Tonbo

Choose this template in Tonbo's browser setup, connect GitHub, create your repository, and install the Slack Agent App. Select a model and complete deployment in the setup flow. In Slack, open the installed Agent and ask: **What can you help us with?** The Agent should read the project brief and answer using its contents.

The template is currently being integrated into Tonbo's browser onboarding. This source directory alone does not prove the hosted setup is available. The connection, deployment, and first Slack conversation must pass the release acceptance described in the platform Epic before that flow is enabled.

## How the Slack service works

`.tonbo` declares `node slack/server.mjs` as the application service and lists the seven secrets it needs: `SLACK_SIGNING_SECRET`, `SLACK_BOT_TOKEN`, `SLACK_BOT_USER_ID`, `SLACK_TEAM_ID`, `TONBO_AGENT_API_KEY`, `TONBO_AGENT_API_ORIGIN` (the Management API the service calls) and `TONBO_IAM_ORIGIN` (the Tonbo origin that exchanges the key for an access token). Tonbo writes them when the Slack App is installed and exports them, with `PORT` and `TONBO_AGENT_ID`, into the service's environment. The service uses only Node's standard library, so no install or build step runs before it starts.

- `POST /slack/events` verifies Slack's signature, answers the URL challenge, drops retries of an event it already acknowledged, and acknowledges within Slack's three-second budget before doing any work.
- A Slack thread is one Tonbo Session and a message is one Turn, derived from the workspace, channel and message timestamp, so a redelivered message never starts a second Turn.
- While the Turn runs, the service follows its event feed and appends assistant text to a native Slack stream. A Turn that fails ends with a notice that names the reason the platform recorded.
- Pressing Slack's stop button aborts the Turn and closes the stream immediately.
- Files attached to a message are downloaded into `/workspace/inbox/slack/<file id>/<file name>` and named in the prompt, so the Agent reads them with its ordinary tools. Files over 50 MiB are refused.
- The service keeps its state in memory. It does not read channel history or post outside the thread it was addressed in.

## Make it yours

- Edit `AGENTS.md` to change the Agent's instructions.
- Edit `pi-package/project-brief.md` to describe your team's project.
- Edit `pi-package/extensions/project-brief.js` to change the custom tool.
- Edit `slack/` to change how the Agent behaves in Slack; run `node --test slack/test/*.test.mjs` to check it.
- Deploy the updated source through Tonbo before expecting Slack conversations to use it.

The managed runtime supplies Pi and platform-issued model credentials; there are no provider tokens or account-specific bindings in this template. Deploying this template through Tonbo's browser setup rewrites `.tonbo` with the model you choose; the service declaration is kept for a Slack-connected Agent and omitted for an Agent without Slack, because the service refuses to start without its secrets.

## Optional local development

With Node.js 22 or newer, run the tool and service tests:

```sh
(cd pi-package && npm test)
node --test slack/test/*.test.mjs
```

To develop interactively, use the Pi CLI in the repository root. For a Tonbo CLI workflow, run `tonbo init`, choose **Use an existing agent**, select the Agent created in the browser, and configure the supported native Pi target before deploying. Local tooling is optional for the browser onboarding experience.

The service runs from revision-isolated application source. Durable files, including Slack attachments and recovery state, belong under `TONBO_WORKSPACE_DIR` (`/workspace`); relative source imports continue to resolve from the deployed revision. Publish source changes through a new deployment.

## Upgrade and recovery behavior

The service acquires named runtime Activity and synchronously saves its Turn checkpoint before acknowledging a prompt event. Ownership lasts through response delivery and Slack status cleanup. Completed checkpoints remain as small deduplication tombstones. A newer revision can serve new requests while the original process finishes its admitted replies; startup recovery skips work still owned by another process. Stop resolves the persisted Turn identity and can reach a Turn on the draining runtime.

Checkpoints preserve the prompt, acknowledged response text, partial event offset and open Slack stream. They are serialized and atomically replaced in Workspace. An uncertain Slack write is retained for reconciliation, rather than replayed or declared successful. Corrupt records are retained and fail closed. Lease expiry does not authorize another process to emit effects.

An unexpected service exit makes runtime health unavailable; the platform physically retires that runtime before another process may recover its named work. In-process service restart is deliberately removed because it cannot establish that the old execution owner is fenced. Snapshot and node-transfer acceptance remain separate from deployment overlap.
