import { describe, it, expect, beforeAll, afterAll, vi } from "vitest";
import type { StartedRabbitMQContainer } from "@testcontainers/rabbitmq";
import { startRabbitContainer, stopRabbitContainer, getAmqpUrl } from "./helpers/container.js";
import { RabbitSingleConnectionHandler } from "../../src/connection/single.js";
import { RabbitConnectionPoolHandler } from "../../src/connection/pool.js";

let container: StartedRabbitMQContainer;
let amqpUrl: string;

beforeAll(async () => {
  container = await startRabbitContainer();
  amqpUrl = getAmqpUrl(container);
});

afterAll(async () => {
  await stopRabbitContainer(container);
});

describe("RabbitSingleConnectionHandler", () => {
  it("connects, exposes live connection, shuts down cleanly and nulls connection", async () => {
    const handler = new RabbitSingleConnectionHandler(amqpUrl);
    await handler.ConnectToService();

    expect(handler.rabbitConnection).not.toBeNull();

    await handler.gracefulShutdown();

    // connection must be null after shutdown — no stale reference
    expect(handler.rabbitConnection).toBeNull();
  });

  it("can reconnect after graceful shutdown — state is fully reset", async () => {
    const handler = new RabbitSingleConnectionHandler(amqpUrl);
    await handler.ConnectToService();
    await handler.gracefulShutdown();

    // should be able to connect again cleanly
    await handler.ConnectToService();
    expect(handler.rabbitConnection).not.toBeNull();

    await handler.gracefulShutdown();
  });

  it("nulls connection on drop and fires all onReconnect callbacks exactly once", async () => {
    const handler = new RabbitSingleConnectionHandler(amqpUrl, {
      reconnectInterval: 300,
      maxReconnectAttempts: 5,
    });
    await handler.ConnectToService();

    const cb1 = vi.fn().mockResolvedValue(undefined);
    const cb2 = vi.fn().mockResolvedValue(undefined);
    handler.onReconnect(cb1);
    handler.onReconnect(cb2);

    const connBefore = handler.rabbitConnection;

    // force drop
    await handler.rabbitConnection!.close();

    // immediately after drop, connection should be null
    expect(handler.rabbitConnection).toBeNull();

    // wait for reconnect
    await new Promise((r) => setTimeout(r, 1000));

    // new connection should be a different object
    expect(handler.rabbitConnection).not.toBeNull();
    expect(handler.rabbitConnection).not.toBe(connBefore);

    // each callback fired exactly once
    expect(cb1).toHaveBeenCalledTimes(1);
    expect(cb2).toHaveBeenCalledTimes(1);

    await handler.gracefulShutdown();
  });

  it("does not reconnect after graceful shutdown when connection closes", async () => {
    const handler = new RabbitSingleConnectionHandler(amqpUrl, {
      reconnectInterval: 300,
      maxReconnectAttempts: 5,
    });
    await handler.ConnectToService();

    const cb = vi.fn().mockResolvedValue(undefined);
    handler.onReconnect(cb);

    await handler.gracefulShutdown();

    // wait to confirm no reconnect happens
    await new Promise((r) => setTimeout(r, 600));

    expect(cb).not.toHaveBeenCalled();
    expect(handler.rabbitConnection).toBeNull();
  });

  it("sets exitCode=1 after exhausting max reconnect attempts", async () => {
    const originalExitCode = process.exitCode;
    const handler = new RabbitSingleConnectionHandler(amqpUrl, {
      reconnectInterval: 100,
      maxReconnectAttempts: 0,
    });
    await handler.ConnectToService();
    await handler.rabbitConnection!.close();
    await new Promise((r) => setTimeout(r, 300));

    expect(process.exitCode).toBe(1);
    process.exitCode = originalExitCode as number;
  });
});

describe("RabbitConnectionPoolHandler", () => {
  it("initializes all connections and acquire returns active ones", async () => {
    const pool = new RabbitConnectionPoolHandler(amqpUrl, 3);
    await pool.ConnectToService();

    const c1 = pool.acquire();
    const c2 = pool.acquire();
    const c3 = pool.acquire();

    expect(c1.rabbitConnection).not.toBeNull();
    expect(c2.rabbitConnection).not.toBeNull();
    expect(c3.rabbitConnection).not.toBeNull();

    await pool.gracefulShutdown();
  });

  it("acquire skips dropped connections and returns next active one", async () => {
    const pool = new RabbitConnectionPoolHandler(amqpUrl, 3);
    await pool.ConnectToService();

    // drop the first connection
    const first = pool.acquire();
    await first.rabbitConnection!.close();
    await new Promise((r) => setTimeout(r, 100));

    // acquire should skip the dropped one and return an active connection
    const active = pool.acquire();
    expect(active.rabbitConnection).not.toBeNull();
    expect(active).not.toBe(first);

    await pool.gracefulShutdown();
  });

  it("throws when all connections are dropped", async () => {
    const pool = new RabbitConnectionPoolHandler(amqpUrl, 2, {
      maxReconnectAttempts: 0,
    });
    await pool.ConnectToService();

    // close both connections sequentially, waiting for null between each
    const c1 = pool.acquire();
    const conn1 = c1.rabbitConnection!;
    await conn1.close();
    await new Promise((r) => setTimeout(r, 100));

    const c2 = pool.acquire();
    const conn2 = c2.rabbitConnection!;
    await conn2.close();
    await new Promise((r) => setTimeout(r, 100));

    expect(() => pool.acquire()).toThrow("[ConnectionPool] No active connections available");

    await pool.gracefulShutdown();
  });

  it("shuts down all connections and clears pool", async () => {
    const pool = new RabbitConnectionPoolHandler(amqpUrl, 3);
    await pool.ConnectToService();
    await pool.gracefulShutdown();

    expect(() => pool.acquire()).toThrow("[ConnectionPool] No connections available");
  });
});
