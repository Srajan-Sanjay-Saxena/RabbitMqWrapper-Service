import type amqp from "amqplib";
import type { RabbitMqBaseClass } from "./connection.js";
import type { PublishOptions } from "./types.js";

export class RabbitProducer {
  private exchangeName: string;
  private routingKey: string;
  private channel: amqp.Channel | null = null;
  private confirmChannel: amqp.ConfirmChannel | null = null;

  public constructor(exchangeName: string, routingKey: string = "") {
    this.exchangeName = exchangeName;
    this.routingKey = routingKey;
  }

  private buildPublishOpts(options: PublishOptions): amqp.Options.Publish {
    return {
      persistent: options.persistent ?? true,
      priority: options.priority,
      expiration: options.expiration,
      correlationId: options.correlationId,
      replyTo: options.replyTo,
      messageId: options.messageId,
      timestamp: options.timestamp ?? Date.now(),
      contentType: options.contentType ?? "application/json",
      headers: options.headers,
    };
  }

  private async getChannel(rabbitBaseInstance: RabbitMqBaseClass) {
    if (!this.channel) {
      this.channel = await rabbitBaseInstance.createChannel();
      this.channel.on("close", () => {
        this.channel = null;
      });
    }
    return this.channel;
  }

  private async getConfirmChannel(rabbitBaseInstance: RabbitMqBaseClass) {
    if (!this.confirmChannel) {
      this.confirmChannel = await rabbitBaseInstance.createConfirmChannel();
      this.confirmChannel.on("close", () => {
        this.confirmChannel = null;
      });
    }
    return this.confirmChannel;
  }

  public async publish(
    rabbitBaseInstance: RabbitMqBaseClass,
    data: Record<string, any>,
    options: PublishOptions = {}
  ) {
    const channel = await this.getChannel(rabbitBaseInstance);
    channel.publish(
      this.exchangeName,
      this.routingKey,
      Buffer.from(JSON.stringify(data)),
      this.buildPublishOpts(options)
    );
  }

  public async publishWithConfirm(
    rabbitBaseInstance: RabbitMqBaseClass,
    data: Record<string, any>,
    options: PublishOptions = {}
  ): Promise<boolean> {
    const channel = await this.getConfirmChannel(rabbitBaseInstance);

    channel.publish(
      this.exchangeName,
      this.routingKey,
      Buffer.from(JSON.stringify(data)),
      this.buildPublishOpts(options)
    );

    try {
      await channel.waitForConfirms();
      return true;
    } catch {
      return false;
    }
  }

  public async close() {
    if (this.channel) {
      await this.channel.close();
      this.channel = null;
    }
    if (this.confirmChannel) {
      await this.confirmChannel.close();
      this.confirmChannel = null;
    }
  }
}
