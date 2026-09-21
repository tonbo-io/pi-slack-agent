import assert from "node:assert/strict";
import { readFile, rm, mkdtemp } from "node:fs/promises";
import { createServer } from "node:http";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { createSlackClient, isSlackFileUrl, safeFileName } from "../slack-api.mjs";

async function withServer(routes, run) {
  const requests = [];
  const server = createServer((request, response) => {
    const chunks = [];
    request.on("data", (chunk) => chunks.push(chunk));
    request.on("end", () => {
      requests.push({
        url: request.url,
        headers: request.headers,
        body: Buffer.concat(chunks).toString(),
      });
      const route = routes[request.url.split("?")[0]];
      if (!route) {
        response.writeHead(404).end();
        return;
      }
      route(request, response);
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const origin = `http://127.0.0.1:${server.address().port}`;
  try {
    await run(origin, requests);
  } finally {
    server.close();
  }
}

test("calls Slack methods with the bot token and surfaces Slack errors", async () => {
  await withServer(
    {
      "/api/chat.startStream": (_request, response) =>
        response.end(JSON.stringify({ ok: true, ts: "1.000001" })),
      "/api/chat.appendStream": (_request, response) =>
        response.end(JSON.stringify({ ok: false, error: "invalid_auth" })),
      "/api/agents.sessions.setStatus": (_request, response) => {
        response.writeHead(429, { "retry-after": "7" }).end();
      },
    },
    async (origin, requests) => {
      const slack = createSlackClient({ botToken: "xoxb-test", origin });
      assert.equal(
        await slack.startStream({
          channelId: "D1",
          threadTs: "1.000000",
          userId: "U1",
          teamId: "T1",
          text: "hi",
        }),
        "1.000001",
      );
      assert.equal(requests[0].headers.authorization, "Bearer xoxb-test");
      assert.deepEqual(JSON.parse(requests[0].body), {
        channel: "D1",
        thread_ts: "1.000000",
        recipient_user_id: "U1",
        recipient_team_id: "T1",
        markdown_text: "hi",
      });
      await assert.rejects(() => slack.appendStream("D1", "1.000001", "x"), {
        code: "invalid_auth",
      });
      await assert.rejects(() => slack.setStatus("D1", "1.000000", "active"), {
        code: "ratelimited",
        retryAfterSeconds: 7,
      });
    },
  );
});

test("downloads a shared file under the inbox with a safe name and enforces the byte limit", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "slack-inbox-"));
  try {
    await withServer(
      {
        "/files/one": (_request, response) => response.end("hello file"),
        "/files/big": (_request, response) => response.end("x".repeat(64)),
        "/api/files.info": (_request, response) =>
          response.end(
            JSON.stringify({
              ok: true,
              file: { url_private_download: "%ORIGIN%/files/one", name: "info.txt" },
            }),
          ),
      },
      async (origin, requests) => {
        const slack = createSlackClient({ botToken: "xoxb-test", origin });
        const saved = await slack.downloadFile(
          { id: "F1", name: "../../etc/passwd", size: 10, url: `${origin}/files/one` },
          directory,
        );
        assert.equal(saved, path.join(directory, "F1", "passwd"));
        assert.equal(await readFile(saved, "utf8"), "hello file");
        assert.equal(requests[0].headers.authorization, "Bearer xoxb-test");
        await assert.rejects(
          () =>
            slack.downloadFile(
              { id: "F2", name: "big.bin", size: null, url: `${origin}/files/big` },
              directory,
              { maxBytes: 32 },
            ),
          { code: "too_large" },
        );
        await assert.rejects(
          () =>
            slack.downloadFile(
              { id: "F3", name: "declared.bin", size: 33, url: `${origin}/files/one` },
              directory,
              { maxBytes: 32 },
            ),
          { code: "too_large" },
        );
        await assert.rejects(readFile(path.join(directory, "F2", "big.bin")));
      },
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("resolves a file without a download URL through files.info", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "slack-inbox-"));
  try {
    await withServer(
      {
        "/files/one": (_request, response) => response.end("hello file"),
        "/api/files.info": (request, response) => {
          const origin = `http://${request.headers.host}`;
          response.end(
            JSON.stringify({
              ok: true,
              file: { url_private_download: `${origin}/files/one`, name: "info.txt" },
            }),
          );
        },
      },
      async (origin) => {
        const slack = createSlackClient({ botToken: "xoxb-test", origin });
        const saved = await slack.downloadFile(
          { id: "F9", name: "", size: null, url: null },
          directory,
        );
        assert.equal(saved, path.join(directory, "F9", "info.txt"));
      },
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("safe names keep one path segment", () => {
  assert.equal(safeFileName("report 2026.pdf"), "report_2026.pdf");
  assert.equal(safeFileName("..", "F1"), "F1");
  assert.equal(safeFileName("", "F1"), "F1");
  assert.equal(safeFileName("a/b/c.txt"), "c.txt");
  assert.equal(safeFileName("x".repeat(300)).length, 128);
});

test("never sends the bot token to a host that is not Slack", async () => {
  let requests = 0;
  const slack = createSlackClient({
    botToken: "xoxb-test",
    fetch: async () => {
      requests += 1;
      return new Response("x");
    },
  });
  for (const url of [
    "https://evil.example/file",
    "http://files.slack.com/file",
    "https://slack.com.evil.example/f",
  ])
    await assert.rejects(
      () => slack.downloadFile({ id: "F1", name: "a", size: 1, url }, "/tmp/x"),
      {
        code: "untrusted_host",
      },
    );
  assert.equal(requests, 0);
  assert.equal(isSlackFileUrl("https://files.slack.com/files-pri/T1-F1/download/a.txt"), true);
  assert.equal(isSlackFileUrl("https://slack.com/x"), true);
  assert.equal(isSlackFileUrl("not a url"), false);
});
