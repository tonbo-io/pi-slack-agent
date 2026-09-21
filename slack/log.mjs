/** Bounded JSON lines on stderr. Never pass secrets or message text. */
export function createLogger(write = (line) => process.stderr.write(line + "\n")) {
  return (event, fields = {}) => {
    const entry = { event, ...fields, at: new Date().toISOString() };
    write(JSON.stringify(entry).slice(0, 2000));
  };
}
