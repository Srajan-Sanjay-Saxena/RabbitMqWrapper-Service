/**
 * Example: Produce with Confirm (Guaranteed Delivery)
 *
 * Demonstrates:
 * - produceWithConfirm() — broker acknowledges receipt
 * - Returns true if confirmed, false if nacked
 * - Use for critical messages (payments, orders)
 */

import {
  RabbitMqBaseClass,
  RabbitProducerExchanger,
} from "../Correct/Rabbit.singleton.correct";

async function main() {
  const rabbit = new RabbitMqBaseClass("amqp://localhost");

  const producer = new RabbitProducerExchanger(
    "payments.exchange",
    {
      paymentId: "PAY-999",
      amount: 149.99,
      currency: "USD",
      userId: "user-123",
    },
    "payment.process"
  );

  const confirmed = await producer.produceWithConfirm(rabbit, {
    persistent: true,
    correlationId: "txn-abc-456",
    headers: { "x-idempotency-key": "idem-PAY-999" },
  });

  if (confirmed) {
    console.log("✓ Broker confirmed — message is safely queued");
  } else {
    console.error("✗ Broker rejected — implement fallback!");
    // Fallback: write to DB outbox, retry later, alert ops, etc.
  }

  await rabbit.gracefulShutdown();
}

main().catch(console.error);
