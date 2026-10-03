import { spawn } from "node:child_process";
import { ENGINE_VERSION, type Command, type Commands } from "./types";

export type ClientOptions = {
  binary?: string;
  registry?: string;
  env?: NodeJS.ProcessEnv;
  timeout_ms?: number;
};
export type CallOptions = { stdin?: string; signal?: AbortSignal };

export class WatcherError extends Error {
  constructor(message: string, readonly stderr: string, readonly exitCode: number | null = null) {
    super(message);
    this.name = "WatcherError";
  }
}
export class ConflictError extends WatcherError {
  constructor(stderr: string) {
    super(`watcher conflict: ${stderr.trim()}`, stderr, 2);
    this.name = "ConflictError";
  }
}

/** A typed process boundary, not an in-process watcher engine. */
export class WatcherClient {
  private checked?: Promise<void>;
  constructor(private readonly options: ClientOptions = {}) {}

  async call<C extends Command>(command: C, input: Commands[C]["input"], options: CallOptions = {}): Promise<Commands[C]["result"]> {
    this.checked ??= this.execute("catalog", {}).then((catalog) => {
      if (catalog.version !== ENGINE_VERSION) {
        throw new WatcherError(`watcher version mismatch: binary ${catalog.version}, client ${ENGINE_VERSION}. Regenerate integrations/types.ts for this binary.`, "");
      }
    });
    // The promise shares only the first-use version check, never command results.
    await this.checked;
    return this.execute(command, input, options);
  }

  private execute<C extends Command>(command: C, input: Commands[C]["input"], options: CallOptions = {}): Promise<Commands[C]["result"]> {
    if (options.signal?.aborted) return Promise.reject(new WatcherError("watcher operation cancelled", ""));
    return new Promise((resolve, reject) => {
      const args = [...command.split(" "), "--input-json", JSON.stringify(input), "--json",
        ...(this.options.registry ? ["--registry", this.options.registry] : [])];
      const child = spawn(this.options.binary ?? "watcher", args, { env: this.options.env, stdio: ["pipe", "pipe", "pipe"] });
      const stdout: Buffer[] = [], stderr: Buffer[] = [];
      let settled = false;
      let inputError: Error | undefined;
      const finish = (error?: Error, value?: Commands[C]["result"]) => {
        if (settled) return;
        settled = true;
        if (timer) clearTimeout(timer);
        options.signal?.removeEventListener("abort", abort);
        error ? reject(error) : resolve(value!);
      };
      const abort = () => { child.kill("SIGTERM"); finish(new WatcherError("watcher operation cancelled", Buffer.concat(stderr).toString("utf8"))); };
      const timer = this.options.timeout_ms === undefined ? undefined : setTimeout(() => {
        child.kill("SIGTERM"); finish(new WatcherError("watcher operation timed out", Buffer.concat(stderr).toString("utf8")));
      }, this.options.timeout_ms);
      options.signal?.addEventListener("abort", abort, { once: true });
      if (options.signal?.aborted) abort();
      child.stdout.on("data", (chunk) => stdout.push(chunk));
      child.stderr.on("data", (chunk) => stderr.push(chunk));
      child.stdin.on("error", (error) => { inputError = error; });
      child.on("error", (error) => finish(new WatcherError(`Could not start watcher: ${error.message}`, Buffer.concat(stderr).toString("utf8"))));
      child.on("close", (code) => {
        const diagnostic = Buffer.concat(stderr).toString("utf8");
        if (code === 2) return finish(new ConflictError(diagnostic));
        if (code !== 0) return finish(new WatcherError(`watcher ${command} failed: ${diagnostic.trim()}`, diagnostic, code));
        if (inputError) return finish(new WatcherError(`watcher input failed: ${inputError.message}`, diagnostic, code));
        try { finish(undefined, JSON.parse(Buffer.concat(stdout).toString("utf8"))); }
        catch (error) { finish(new WatcherError(`watcher returned invalid JSON: ${error instanceof Error ? error.message : String(error)}`, diagnostic, code)); }
      });
      child.stdin.end(options.stdin);
    });
  }
}
