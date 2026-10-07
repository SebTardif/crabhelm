import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import test from "node:test";
import type { Pool } from "pg";
import WebSocket, { WebSocketServer } from "ws";
import { loadAwsConfig, type AwsConfig } from "../../aws/config.js";
import {
  AwsCoordinatorDirectory,
  MAX_PENDING_RUNTIME_FRAMES,
  type AwsClawCoordinator,
} from "../../aws/postgres-coordinator.js";
import { bindAwsRuntimeUpgrade, createAwsHttpServer } from "../../aws/server.js";
import type { RuntimeTicketClaims } from "../../src/governance-types.js";
import { signClaims } from "../../worker/security.js";

const clawId = "685b2bda-351e-450b-a91c-45938c54454f";
const signingSecret = "r".repeat(32);
const vaultMasterKey = Buffer.alloc(32, 7).toString("base64url");

type RecordedQuery = { sql: string; values: unknown[] };
type FakeResponse = { rows?: unknown[]; rowCount?: number | null };

class FakePool {
  readonly queries: RecordedQuery[] = [];
  #removedChecks = 0;
  holdActive: Promise<void> = Promise.resolve();
  holdOfferRelease: Promise<void> | undefined;
  armReconnectHold = false;
  #awaitReconnectHold = false;

  async query(sql: string, values: unknown[] = []): Promise<unknown> {
    const normalized = sql.replace(/\s+/gu, " ").trim();
    this.queries.push({ sql: normalized, values });
    if (normalized.includes("SELECT removed_at")) {
      this.#removedChecks += 1;
      if (this.armReconnectHold && !this.#awaitReconnectHold) {
        this.#awaitReconnectHold = true;
      } else if (this.#awaitReconnectHold) {
        this.#awaitReconnectHold = false;
        this.armReconnectHold = false;
        await this.holdActive;
      } else if (this.#removedChecks === 2) {
        await this.holdActive;
        await delay(50);
      } else if (this.#removedChecks > 2) {
        await delay(50);
      }
      return result([]);
    }
    if (normalized.startsWith("UPDATE") && normalized.includes("crabhelm_coordinator_runtime_tickets")) {
      return result([], 1);
    }
    if (
      this.holdOfferRelease &&
      normalized.startsWith("UPDATE") &&
      normalized.includes("SET status = 'pending'") &&
      normalized.includes("status = 'offered'")
    ) {
      const hold = this.holdOfferRelease;
      this.holdOfferRelease = undefined;
      await hold;
    }
    if (normalized.includes("reset_generation")) return result([{ reset_generation: "1" }], 1);
    if (normalized.includes("COUNT(*)")) return result([{ count: "0" }], 1);
    return result([]);
  }

  async connect(): Promise<unknown> {
    return {
      query: (sql: string, values?: unknown[]) => this.query(sql, values),
      release() {},
    };
  }

  asPool(): Pool {
    return this as unknown as Pool;
  }
}

test("runtime upgrade reads a job claim sent as the socket opens", async (t) => {
  const harness = await openHarness();
  t.after(() => harness.close());
  const ticket = await signClaims<RuntimeTicketClaims>(signingSecret, {
    typ: "runtime-ticket",
    aud: "crabhelm-runtime-connect",
    clawId,
    runtimeId: "runtime-a",
    refreshJti: "refresh-a",
  }, 30);
  const received: Array<{ type?: string }> = [];
  const client = new WebSocket(
    `ws://127.0.0.1:${harness.port}/api/runtime/connect?clawId=${clawId}`,
    ["crabhelm.runtime.v1", `crabhelm.ticket.${ticket}`],
  );
  t.after(() => closeClient(client));
  const failure = new Promise<string>((resolve) => {
    client.once("unexpected-response", (_request, response) => {
      resolve(`unexpected response ${response.statusCode ?? 0}`);
    });
    client.once("error", (error: Error) => resolve(error.message));
  });
  client.on("open", () => {
    client.send(JSON.stringify({ type: "job.claim" }));
    client.send(JSON.stringify({ type: "runtime.heartbeat" }));
  });
  client.on("message", (data) => {
    received.push(JSON.parse(data.toString()) as { type?: string });
  });

  const ready = waitFor(() => received.some((message) => message.type === "runtime.ready"), 2_000);
  const outcome = await Promise.race([
    ready.then(() => "ready" as const),
    failure.then((message) => message),
  ]);
  assert.equal(outcome, "ready");
  assert.deepEqual(received.map((message) => message.type), [
    "job.none",
    "runtime.heartbeat",
    "runtime.ready",
  ]);
  assert.equal(claimReads(harness.pool), 1);
});

test("early runtime frames stay ordered when attachment is delayed", async (t) => {
  let release = () => {};
  const hold = new Promise<void>((resolve) => {
    release = resolve;
  });
  const harness = await openHarness(hold);
  t.after(() => harness.close());
  const ticket = await signClaims<RuntimeTicketClaims>(signingSecret, {
    typ: "runtime-ticket",
    aud: "crabhelm-runtime-connect",
    clawId,
    runtimeId: "runtime-ordered",
    refreshJti: "refresh-ordered",
  }, 30);
  const received: Array<{ type?: string }> = [];
  const client = new WebSocket(
    `ws://127.0.0.1:${harness.port}/api/runtime/connect?clawId=${clawId}`,
    ["crabhelm.runtime.v1", `crabhelm.ticket.${ticket}`],
  );
  t.after(() => closeClient(client));
  client.on("message", (data) => {
    received.push(JSON.parse(data.toString()) as { type?: string });
  });
  await new Promise<void>((resolve, reject) => {
    client.once("open", () => resolve());
    client.once("error", reject);
  });
  client.send(JSON.stringify({ type: "job.claim" }));
  client.send(JSON.stringify({ type: "runtime.heartbeat" }));
  await delay(50);
  release();
  await waitFor(() => received.some((message) => message.type === "runtime.ready"), 2_000);
  assert.deepEqual(received.map((message) => message.type), [
    "job.none",
    "runtime.heartbeat",
    "runtime.ready",
  ]);
});

test("too many frames before attachment close the runtime socket", async (t) => {
  let release = () => {};
  const hold = new Promise<void>((resolve) => {
    release = resolve;
  });
  const harness = await openHarness(hold);
  t.after(() => harness.close());
  const ticket = await signClaims<RuntimeTicketClaims>(signingSecret, {
    typ: "runtime-ticket",
    aud: "crabhelm-runtime-connect",
    clawId,
    runtimeId: "runtime-overflow",
    refreshJti: "refresh-overflow",
  }, 30);
  const received: Array<{ type?: string }> = [];
  const client = new WebSocket(
    `ws://127.0.0.1:${harness.port}/api/runtime/connect?clawId=${clawId}`,
    ["crabhelm.runtime.v1", `crabhelm.ticket.${ticket}`],
  );
  t.after(() => closeClient(client));
  client.on("message", (data) => {
    received.push(JSON.parse(data.toString()) as { type?: string });
  });
  const closed = new Promise<number>((resolve) => {
    client.once("close", (code) => resolve(code));
  });
  await new Promise<void>((resolve, reject) => {
    client.once("open", () => resolve());
    client.once("error", reject);
  });
  for (let index = 0; index < MAX_PENDING_RUNTIME_FRAMES + 1; index += 1) {
    client.send(JSON.stringify({ type: "job.claim", index }));
  }
  await delay(50);
  release();
  const code = await Promise.race([
    closed,
    delay(2_000).then(() => -1),
  ]);
  assert.equal(code, 1009);
  assert.equal(received.some((message) => message.type === "runtime.ready"), false);
  assert.equal(claimReads(harness.pool), 0);
});

test("an overflowed reconnect leaves the healthy runtime connected", async (t) => {
  const harness = await openHarness();
  t.after(() => harness.close());
  const claims = {
    typ: "runtime-ticket" as const,
    aud: "crabhelm-runtime-connect" as const,
    clawId,
    runtimeId: "runtime-healthy",
    refreshJti: "refresh-healthy",
  };
  const firstTicket = await signClaims<RuntimeTicketClaims>(signingSecret, claims, 30);
  const first = new WebSocket(
    `ws://127.0.0.1:${harness.port}/api/runtime/connect?clawId=${clawId}`,
    ["crabhelm.runtime.v1", `crabhelm.ticket.${firstTicket}`],
  );
  t.after(() => closeClient(first));
  const firstMessages: Array<{ type?: string }> = [];
  let firstClosed = false;
  first.on("message", (data) => {
    firstMessages.push(JSON.parse(data.toString()) as { type?: string });
  });
  first.on("close", () => { firstClosed = true; });
  await new Promise<void>((resolve, reject) => {
    first.once("open", () => resolve());
    first.once("error", reject);
  });
  await waitFor(() => firstMessages.some((message) => message.type === "runtime.ready"), 2_000);
  const offerReleases = () => harness.pool.queries.filter((query) => query.sql.includes("status = 'offered'")).length;
  const offersAtReady = offerReleases();
  let release = () => {};
  const hold = new Promise<void>((resolve) => { release = resolve; });
  harness.pool.holdActive = hold;
  harness.pool.armReconnectHold = true;
  const secondTicket = await signClaims<RuntimeTicketClaims>(signingSecret, {
    ...claims,
    refreshJti: "refresh-healthy-2",
  }, 30);
  const second = new WebSocket(
    `ws://127.0.0.1:${harness.port}/api/runtime/connect?clawId=${clawId}`,
    ["crabhelm.runtime.v1", `crabhelm.ticket.${secondTicket}`],
  );
  t.after(() => closeClient(second));
  const secondClosed = new Promise<number>((resolve) => {
    second.once("close", (code) => resolve(code));
  });
  await new Promise<void>((resolve, reject) => {
    second.once("open", () => resolve());
    second.once("error", reject);
  });
  for (let index = 0; index < MAX_PENDING_RUNTIME_FRAMES + 1; index += 1) {
    second.send(JSON.stringify({ type: "job.claim", index }));
  }
  await delay(50);
  release();
  const code = await Promise.race([secondClosed, delay(2_000).then(() => -1)]);
  await delay(100);
  assert.equal(code, 1009);
  assert.equal(firstClosed, false);
  assert.equal(first.readyState, WebSocket.OPEN);
  assert.equal(offerReleases(), offersAtReady);
});

test("frames during offer release do not drop the replacement runtime", async (t) => {
  const harness = await openHarness();
  t.after(() => harness.close());
  const claims = {
    typ: "runtime-ticket" as const,
    aud: "crabhelm-runtime-connect" as const,
    clawId,
    runtimeId: "runtime-release",
    refreshJti: "refresh-release",
  };
  const firstTicket = await signClaims<RuntimeTicketClaims>(signingSecret, claims, 30);
  const first = new WebSocket(
    `ws://127.0.0.1:${harness.port}/api/runtime/connect?clawId=${clawId}`,
    ["crabhelm.runtime.v1", `crabhelm.ticket.${firstTicket}`],
  );
  t.after(() => closeClient(first));
  const firstMessages: Array<{ type?: string }> = [];
  let firstCode = 0;
  first.on("message", (data) => {
    firstMessages.push(JSON.parse(data.toString()) as { type?: string });
  });
  first.on("close", (code) => { firstCode = code; });
  await new Promise<void>((resolve, reject) => {
    first.once("open", () => resolve());
    first.once("error", reject);
  });
  await waitFor(() => firstMessages.some((message) => message.type === "runtime.ready"), 2_000);
  let release = () => {};
  harness.pool.holdOfferRelease = new Promise<void>((resolve) => { release = resolve; });
  const secondTicket = await signClaims<RuntimeTicketClaims>(signingSecret, {
    ...claims,
    refreshJti: "refresh-release-2",
  }, 30);
  const second = new WebSocket(
    `ws://127.0.0.1:${harness.port}/api/runtime/connect?clawId=${clawId}`,
    ["crabhelm.runtime.v1", `crabhelm.ticket.${secondTicket}`],
  );
  t.after(() => closeClient(second));
  const secondMessages: Array<{ type?: string }> = [];
  let secondCode = 0;
  second.on("message", (data) => {
    secondMessages.push(JSON.parse(data.toString()) as { type?: string });
  });
  second.on("close", (code) => { secondCode = code; });
  await new Promise<void>((resolve, reject) => {
    second.once("open", () => resolve());
    second.once("error", reject);
  });
  const releasesBeforeFlood = offerReleaseCount(harness.pool);
  await waitFor(() => offerReleaseCount(harness.pool) > releasesBeforeFlood, 2_000);
  for (let index = 0; index < MAX_PENDING_RUNTIME_FRAMES + 1; index += 1) {
    second.send(JSON.stringify({ type: "job.claim", index }));
  }
  await delay(50);
  release();
  await waitFor(() => secondMessages.some((message) => message.type === "runtime.ready"), 2_000);
  await waitFor(() => claimReads(harness.pool) > 0, 2_000);
  assert.equal(secondCode, 0);
  assert.equal(second.readyState, WebSocket.OPEN);
  assert.equal(firstCode, 4001);
});

test("failed runtime upgrade does not enter the coordinator", async (t) => {
  const harness = await openHarness();
  t.after(() => harness.close());
  const client = new WebSocket(`ws://127.0.0.1:${harness.port}/not-runtime`);
  t.after(() => closeClient(client));
  client.on("error", () => undefined);
  const status = await new Promise<number>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timed out waiting for the upgrade response")), 2_000);
    client.once("unexpected-response", (_request, response) => {
      clearTimeout(timer);
      resolve(response.statusCode ?? 0);
    });
    client.once("open", () => {
      clearTimeout(timer);
      reject(new Error("failed upgrade was resumed"));
    });
  });
  assert.equal(status, 404);
  assert.equal(claimReads(harness.pool), 0);
});

async function openHarness(holdActive?: Promise<void>): Promise<{ port: number; pool: FakePool; close(): Promise<void> }> {
  const pool = new FakePool();
  if (holdActive) pool.holdActive = holdActive;
  const config = loadAwsConfig(runtimeEnvironment());
  const coordinators = new AwsCoordinatorDirectory({
    pool: pool.asPool(),
    vaultMasterKey,
    runtimeSigningSecret: signingSecret,
    now: () => 1_800_000_000_000,
  });
  const sockets = new Map<WebSocket, AwsClawCoordinator>();
  const webSocketServer = new WebSocketServer({
    noServer: true,
    clientTracking: true,
    perMessageDeflate: false,
    maxPayload: 64 * 1024,
    handleProtocols(protocols) {
      return protocols.has("crabhelm.runtime.v1") ? "crabhelm.runtime.v1" : false;
    },
  });
  const server = createAwsHttpServer((_request, response) => {
    response.statusCode = 404;
    response.end("not found");
  });
  bindAwsRuntimeUpgrade(server, {
    config,
    env: {
      RUNTIME_SIGNING_SECRET: signingSecret,
      RUNTIME_URL: config.controlPlane.RUNTIME_URL,
      CLAW_COORDINATOR: coordinators,
    } as unknown as Env,
    coordinators,
    webSocketServer,
    sockets,
    shuttingDown: () => false,
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address() as AddressInfo;
  config.controlPlane.RUNTIME_URL = `https://127.0.0.1:${address.port}`;
  return {
    port: address.port,
    pool,
    async close() {
      for (const socket of webSocketServer.clients) socket.terminate();
      await Promise.race([
        new Promise<void>((resolve) => {
          webSocketServer.close(() => resolve());
        }),
        delay(200),
      ]);
      server.closeAllConnections();
      if (!server.listening) return;
      await Promise.race([
        new Promise<void>((resolve) => {
          server.close(() => resolve());
        }),
        delay(200),
      ]);
    },
  };
}

function offerReleaseCount(pool: FakePool): number {
  return pool.queries.filter((query) =>
    query.sql.startsWith("UPDATE") &&
    query.sql.includes("SET status = 'pending'") &&
    query.sql.includes("status = 'offered'")
  ).length;
}

function claimReads(pool: FakePool): number {
  return pool.queries.filter((query) =>
    query.sql.startsWith("SELECT") &&
    query.sql.includes("status = 'offered'") &&
    query.sql.includes("runtime_id = $2")
  ).length;
}

function result(rows: unknown[], rowCount = rows.length): FakeResponse & { rows: unknown[]; rowCount: number } {
  return {
    command: "",
    fields: [],
    oid: 0,
    rows,
    rowCount,
  } as FakeResponse & { rows: unknown[]; rowCount: number };
}

function runtimeEnvironment(): Record<string, string> {
  const digest = "a".repeat(64);
  return {
    AWS_REGION: "us-west-2",
    AWS_LOAD_BALANCER_ARN: "arn:aws:elasticloadbalancing:us-west-2:123456789012:loadbalancer/app/crabhelm/abc123",
    DATABASE_HOST: "database.internal",
    DATABASE_PORT: "5432",
    DATABASE_NAME: "crabhelm",
    DATABASE_USER: "crabhelm",
    DATABASE_PASSWORD: "password",
    AWS_APPLIANCES_BUCKET: "crabhelm-prod-appliances",
    AWS_OAUTH_VAULT_BUCKET: "crabhelm-prod-oauth-vault",
    AWS_AUDIT_ARCHIVE_BUCKET: "crabhelm-prod-audit-archive",
    AWS_AUDIT_QUEUE_URL: "https://sqs.us-west-2.amazonaws.com/123456789012/crabhelm-audit",
    PUBLIC_URL: "https://crabhelm.example.com",
    RUNTIME_URL: "https://crabhelm-runtime.example.com",
    OIDC_ISSUER: "https://identity.example.com/oauth2/default",
    OIDC_CLIENT_ID: "access-client-id",
    ALB_SESSION_COOKIE_NAME: "AWSELBAuthSessionCookie-1",
    ACCESS_ADMIN_EMAILS: "admin@example.com",
    CRABHELM_PROBE_EMAIL: "probe@example.com",
    GITHUB_OAUTH_CLIENT_ID: "github-client-id",
    GITHUB_OAUTH_CLIENT_SECRET: "github-client-secret",
    CRABBOX_URL: "https://crabbox.example.com/control",
    CRABBOX_TOKEN: "crabbox-token",
    CRABBOX_TARGET_ID: "aws-west",
    CRABBOX_TARGET_LABEL: "AWS US West",
    CRABBOX_PROFILE: "openclaw-core",
    CRABBOX_TTL_SECONDS: "14400",
    CRABBOX_IDLE_TIMEOUT_SECONDS: "7200",
    CRABHELM_SLACK: "off",
    CRABHELM_CLAWROUTER: "off",
    CRABHELM_PROMETHEUS: "off",
    OPENAI_API_KEY: "openai-key",
    NODE_RUNTIME_SHA256: digest,
    APPLIANCE_ARCHIVE_SHA256: digest,
    APPLIANCE_MANIFEST_SHA256: digest,
    BOOTSTRAP_SIGNING_SECRET: signingSecret,
    SESSION_SIGNING_SECRET: signingSecret,
    INVOCATION_SIGNING_SECRET: signingSecret,
    RUNTIME_SIGNING_SECRET: signingSecret,
    VAULT_MASTER_KEY: vaultMasterKey,
  };
}

function closeClient(client: WebSocket): void {
  if (client.readyState === WebSocket.CLOSED) return;
  client.terminate();
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

function waitFor(predicate: () => boolean, timeoutMs: number): Promise<void> {
  const started = Date.now();
  return new Promise((resolve, reject) => {
    const timer = setInterval(() => {
      if (predicate()) {
        clearInterval(timer);
        resolve();
        return;
      }
      if (Date.now() - started >= timeoutMs) {
        clearInterval(timer);
        reject(new Error("timed out waiting for the runtime socket"));
      }
    }, 10);
  });
}
