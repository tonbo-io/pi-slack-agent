import { randomUUID } from "node:crypto";
import { mkdir, open, readdir, readFile, rename, rm } from "node:fs/promises";
import path from "node:path";

export const SLACK_INFLIGHT = path.join(
  process.env.TONBO_WORKSPACE_DIR || "/workspace",
  ".slack/inflight",
);
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const fields = [
  "turnId",
  "sessionId",
  "channelId",
  "threadTs",
  "userId",
  "streamTs",
  "streamOpenedAt",
  "messageChars",
  "streamed",
  "cursor",
  "startedAt",
  "prompt",
  "command",
  "closed",
  "pendingEffect",
  "processingEvent",
  "completed",
];

/** The named Activity owns each file across processes. Within a process,
 * serialize every write and completion so an old save cannot resurrect work.
 * Completed tombstones make a delayed duplicate harmless after lease release. */
export function createInflightStore({ directory = SLACK_INFLIGHT } = {}) {
  const writes = new Map();
  const file = (id) => {
    if (!UUID.test(id)) throw new Error("Invalid checkpoint identity.");
    return path.join(directory, `${id}.json`);
  };
  function serial(id, action) {
    const operation = (writes.get(id) || Promise.resolve()).catch(() => {}).then(action);
    writes.set(id, operation);
    return operation.finally(() => {
      if (writes.get(id) === operation) writes.delete(id);
    });
  }
  async function get(id) {
    let bytes;
    try {
      bytes = await readFile(file(id), "utf8");
    } catch (error) {
      if (error.code === "ENOENT") return null;
      throw error;
    }
    const record = JSON.parse(bytes);
    if (record.version !== 2 || record.turnId !== id || !UUID.test(record.sessionId))
      throw new Error("Invalid Slack checkpoint; preserve it for reconciliation.");
    return record;
  }
  async function write(record) {
    await mkdir(directory, { recursive: true });
    const target = file(record.turnId);
    const temporary = `${target}.${randomUUID()}.tmp`;
    try {
      const handle = await open(temporary, "wx", 0o600);
      try {
        await handle.writeFile(JSON.stringify(record));
        await handle.sync();
      } finally {
        await handle.close();
      }
      await rename(temporary, target);
      const parent = await open(directory, "r");
      try {
        await parent.sync();
      } finally {
        await parent.close();
      }
    } finally {
      await rm(temporary, { force: true });
    }
  }
  return {
    get,
    save(turn) {
      const record = structuredClone({
        version: 2,
        ...Object.fromEntries(fields.map((name) => [name, turn[name]])),
      });
      return serial(turn.turnId, async () => {
        if ((await get(turn.turnId))?.completed) return;
        await write(record);
      });
    },
    complete(turn) {
      return serial(turn.turnId, () =>
        write({
          version: 2,
          turnId: turn.turnId,
          sessionId: turn.sessionId,
          completed: true,
        }),
      );
    },
    async list() {
      let names;
      try {
        names = await readdir(directory);
      } catch (error) {
        if (error.code === "ENOENT") return [];
        throw error;
      }
      const result = [];
      for (const name of names.filter((value) => value.endsWith(".json"))) {
        const record = await get(name.slice(0, -5));
        if (record && !record.completed) result.push(record);
      }
      return result;
    },
  };
}
