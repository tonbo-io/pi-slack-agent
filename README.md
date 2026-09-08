# Pi Slack Agent

A Pi Agent with a small, editable project brief and a custom tool that reads it. Tonbo's Slack connection delivers conversations to the Agent and sends its replies back to Slack. This repository contains your Agent instructions and tool code; Slack app credentials stay with Tonbo.

## Use in Tonbo

Choose this template in Tonbo's browser setup, connect GitHub, create your repository, and install the Slack Agent App. Select a model and complete deployment in the setup flow. In Slack, open the installed Agent and ask: **What can you help us with?** The Agent should read the project brief and answer using its contents.

The template is currently being integrated into Tonbo's browser onboarding. This source directory alone does not prove the hosted setup is available. The connection, deployment, and first Slack conversation must pass the release acceptance described in the platform Epic before that flow is enabled.

## Make it yours

- Edit `AGENTS.md` to change the Agent's instructions.
- Edit `pi-package/project-brief.md` to describe your team's project.
- Edit `pi-package/extensions/project-brief.js` to change the custom tool.
- Deploy the updated source through Tonbo before expecting Slack conversations to use it.

The example does not read your Slack channel history or send messages to other channels. Its custom tool reads only the project brief. The managed runtime supplies Pi and platform-issued model credentials; there are no provider tokens or account-specific bindings in this template.

## Optional local development

With Node.js 22 or newer, run the tool test:

```sh
cd pi-package
npm test
```

To develop interactively, use the Pi CLI in the repository root. For a Tonbo CLI workflow, run `tonbo init`, choose **Use an existing agent**, select the Agent created in the browser, and configure the supported native Pi target before deploying. Local tooling is optional for the browser onboarding experience.
