/**
 * Example: Auto-Reconnection with Full Re-initialization
 *
 * Demonstrates:
 * - onReconnect() to re-establish exchanges, queues, and consumers after connection drops
 * - Complete lifecycle: setup → consume → reconnect → re-setup → resume consuming
 *
 * Test this by:
 *   1. Start this script
 *   2. Restart RabbitMQ: `docker restart rabbitmq`
 *   3. Watch it reconnect and resume consuming automatically
 */

import {
  RabbitMqBaseClass,
  RabbitMqQueueExchange,
  RabbitProducerExchanger,
} from "../src/index.js";

// --- Exchange Setup ---

class OrderExchange extends RabbitMqQueueExchange {
  constructor() {
    super("orders.exchange", "topic", { durable: true });
  }

  async setup(rabbit: RabbitMqBaseClass) {
    await this.startChannelization(rabbit);
    await this.createExchange();

    // DLQ for failed orders
    await this.createQueue("orders.dlq", "order.failed.#", { durable: true });

    // Main processing queue with DLX
    await this.createDeadLetterQueue(
      "orders.process",
      "orders.exchange",
      "order.failed",
      60000
    );

    console.log("[Setup] Exchange and queues asserted");
  }
}

// --- Consumer ---

async function startConsumer(rabbit: RabbitMqBaseClass) {
  const consumer = new RabbitProducerExchanger("orders.exchange", {});

  await consumer.consumeMessage(
    rabbit,
    "orders.process",
    async (data, msg) => {
      const attempt = (msg.properties.headers?.["x-retry-count"] ?? 0) + 1;
      console.log(
        `[Consumer] Processing order ${data.orderId} (attempt ${attempt})`
      );

      // Simulate work
      await new Promise((r) => setTimeout(r, 300));
      console.log(`[Consumer] Order ${data.orderId} done ✓`);
    },
    {
      workerCount: 2,
      prefetchCount: 1,
      requeueOnFailure: true,
      retryLimit: 3,
    }
  );

  console.log("[Consumer] 2 workers consuming from orders.process");
}

// --- Main ---

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost", {
    heartbeat: 10,
    reconnectInterval: 2000,
    maxReconnectAttempts: 20,
  });

  await rabbit.ConnectToService();
  console.log("[Main] Connected to RabbitMQ");

  // Initial setup
  const exchange = new OrderExchange();
  await exchange.setup(rabbit);
  await startConsumer(rabbit);

  // Register reconnection handler — re-assert everything after connection drops
  rabbit.onReconnect(async () => {
    console.log("[Reconnect] Connection restored. Re-initializing...");

    // Re-assert exchanges and queues (idempotent — safe to call again)
    await exchange.setup(rabbit);
    console.log("[Reconnect] Exchanges and queues re-asserted");

    // Note: consumers are automatically re-established by consumeMessage()
    // because it internally registers its own onReconnect callback.
    // You only need to re-assert exchanges/queues here.

    console.log("[Reconnect] All resources re-initialized ✓");
  });

  // Publish a test message every 5 seconds to verify things work
  setInterval(async () => {
    try {
      const producer = new RabbitProducerExchanger(
        "orders.exchange",
        { orderId: `ORD-${Date.now()}`, item: "Widget", qty: 1 },
        "order.created"
      );
      await producer.produceMessage(rabbit, { persistent: true });
      console.log("[Publisher] Test message sent");
    } catch (err) {
      console.warn("[Publisher] Failed to publish (connection down?):", (err as Error).message);
    }
  }, 5000);

  // Graceful shutdown
  const shutdown = async () => {
    console.log("\n[Shutdown] Closing connection...");
    await rabbit.gracefulShutdown();
    process.exit(0);
  };

  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);

  console.log("\n[Main] Service running. Press Ctrl+C to stop.");
  console.log("[Main] Restart RabbitMQ to test reconnection.\n");
}

main().catch(console.error);
