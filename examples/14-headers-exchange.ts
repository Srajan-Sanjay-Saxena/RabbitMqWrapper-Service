/**
 * Example: Headers Exchange (Route by Headers, not Routing Key)
 *
 * Demonstrates:
 * - Exchange type "headers" — routing based on message headers
 * - Binding with x-match: "all" (all headers must match) or "any"
 * - Useful when routing logic is more complex than a string pattern
 */

import {
  RabbitMqBaseClass,
  RabbitMqQueueExchange,
  RabbitProducerExchanger,
} from "../Correct/Rabbit.singleton.correct";

class EventExchange extends RabbitMqQueueExchange {
  constructor() {
    super("events.headers", "headers", { durable: true });
  }

  async setup(rabbit: RabbitMqBaseClass) {
    await this.startChannelization(rabbit);
    await this.createExchange();

    // Queue that only receives messages where BOTH headers match
    await this.createQueue(
      "events.critical-errors",
      "",  // routing key ignored for headers exchange
      { durable: true },
      undefined,
      {
        "x-match": "all",          // ALL headers must match
        "severity": "critical",
        "type": "error",
      }
    );

    // Queue that receives messages where ANY header matches
    await this.createQueue(
      "events.all-errors",
      "",
      { durable: true },
      undefined,
      {
        "x-match": "any",          // ANY header can match
        "type": "error",
        "type2": "warning",
      }
    );

    console.log("Headers exchange setup complete");
  }
}

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");
  await rabbit.ConnectToService();

  const exchange = new EventExchange();
  await exchange.setup(rabbit);

  // This message matches "events.critical-errors" (both headers match)
  const criticalError = new RabbitProducerExchanger(
    "events.headers",
    { message: "Database connection pool exhausted" },
    "" // routing key doesn't matter for headers exchange
  );

  await criticalError.produceMessage(rabbit, {
    headers: { severity: "critical", type: "error" },
  });
  console.log("Critical error published → routes to both queues");

  // This message only matches "events.all-errors" (only type matches)
  const warning = new RabbitProducerExchanger(
    "events.headers",
    { message: "High memory usage detected" },
    ""
  );

  await warning.produceMessage(rabbit, {
    headers: { severity: "warning", type: "error" },
  });
  console.log("Warning published → routes to all-errors only");

  await rabbit.gracefulShutdown();
}

main().catch(console.error);
