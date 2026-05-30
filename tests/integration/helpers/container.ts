import { RabbitMQContainer, type StartedRabbitMQContainer } from "@testcontainers/rabbitmq";

export async function startRabbitContainer(): Promise<StartedRabbitMQContainer> {
  return new RabbitMQContainer("rabbitmq:3-management").start();
}

export async function stopRabbitContainer(container: StartedRabbitMQContainer) {
  await container.stop();
}

export function getAmqpUrl(container: StartedRabbitMQContainer): string {
  return container.getAmqpUrl();
}
