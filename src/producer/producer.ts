import type amqp from "amqplib";
import { ChannelManager } from "../channel/manager.js";
import { RabbitLogger } from "../logger/logger.js";
import type { PublishOptions } from "../types.js";
import type { IRabbitConnection } from "../connection/single.js";

interface BufferedMessage<T extends Record<string, any>> {
  data: T;
  options: PublishOptions;
  resolve: (value: boolean) => void;
  reject: (err: Error) => void;
  confirm: boolean;
}

export class RabbitProducer<T extends Record<string, any> = Record<string, any>> {
  private exchangeName: string;
  private routingKey: string;
  private buffer: BufferedMessage<T>[] = [];
  private maxBufferSize: number;
  private channel: amqp.Channel | null = null;
  private confirmChannel: amqp.ConfirmChannel | null = null;
  private connInstance: IRabbitConnection;
  private logger: RabbitLogger;

  public constructor(
    connInstance: IRabbitConnection,
    exchangeName: string,
    routingKey: string = "",
    maxBufferSize: number = 10000,
  ) {
    this.connInstance = connInstance;
    this.exchangeName = exchangeName;
    this.routingKey = routingKey;
    this.maxBufferSize = maxBufferSize;
    this.logger = new RabbitLogger();
    connInstance.onReconnect(async () => {
      this.logger.info("Reconnected — resetting channels and flushing buffer", "Producer", {
        exchange: this.exchangeName,
        buffered: this.buffer.length,
      });
      this.channel = null;
      this.confirmChannel = null;
      await this.flushBuffer();
    });
  }

  public addLogger(logger: RabbitLogger) {
    this.logger = logger;
  }

  private async getChannel(): Promise<amqp.Channel> {
    const conn = this.connInstance.rabbitConnection;
    if (!conn) throw new Error("[Producer] No active connection");
    if (!this.channel) {
      this.channel = await ChannelManager.createChannel(conn, () => {
        this.logger.warn("Producer channel closed", "Producer", { exchange: this.exchangeName });
        this.channel = null;
      });
      this.logger.debug("Producer channel created", "Producer", { exchange: this.exchangeName });
    }
    return this.channel;
  }

  private async getConfirmChannel(): Promise<amqp.ConfirmChannel> {
    const conn = this.connInstance.rabbitConnection;
    if (!conn) throw new Error("[Producer] No active connection");
    if (!this.confirmChannel) {
      this.confirmChannel = await ChannelManager.createConfirmChannel(conn, () => {
        this.logger.warn("Producer confirm channel closed", "Producer", { exchange: this.exchangeName });
        this.confirmChannel = null;
      });
      this.logger.debug("Producer confirm channel created", "Producer", { exchange: this.exchangeName });
    }
    return this.confirmChannel;
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

  private async waitForDrain(channel: amqp.Channel): Promise<void> {
    this.logger.warn("TCP buffer full, waiting for drain", "Producer", { exchange: this.exchangeName });
    return new Promise((resolve) => channel.once("drain", resolve));
  }

  private async flushBuffer() {
    if (this.buffer.length === 0) return;
    this.logger.info("Flushing buffered messages", "Producer", { count: this.buffer.length, exchange: this.exchangeName });
    const pending = [...this.buffer];
    this.buffer = [];

    for (const msg of pending) {
      try {
        if (msg.confirm) {
          msg.resolve(await this.publishWithConfirm(msg.data, msg.options));
        } else {
          await this.publish(msg.data, msg.options);
          msg.resolve(true);
        }
      } catch (err) {
        this.logger.error("Failed to flush buffered message", "Producer", { exchange: this.exchangeName, err });
        msg.reject(err as Error);
      }
    }
    this.logger.info("Buffer flushed", "Producer", { exchange: this.exchangeName });
  }

  public async publish(data: T, options: PublishOptions = {}): Promise<boolean> {
    try {
      const channel = await this.getChannel();
      const content = Buffer.from(JSON.stringify(data));
      const written = channel.publish(this.exchangeName, this.routingKey, content, this.buildPublishOpts(options));
      if (!written) await this.waitForDrain(channel);
      return true;
    } catch {
      this.logger.warn("Publish failed, buffering message", "Producer", { exchange: this.exchangeName, routingKey: this.routingKey });
      return this.bufferMessage(data, options, false);
    }
  }

  public async publishWithConfirm(data: T, options: PublishOptions = {}): Promise<boolean> {
    try {
      const channel = await this.getConfirmChannel();
      const content = Buffer.from(JSON.stringify(data));
      const written = channel.publish(this.exchangeName, this.routingKey, content, this.buildPublishOpts(options));
      if (!written) await this.waitForDrain(channel as unknown as amqp.Channel);
      await channel.waitForConfirms();
      this.logger.debug("Broker confirmed message", "Producer", { exchange: this.exchangeName, routingKey: this.routingKey });
      return true;
    } catch (err) {
      this.logger.error("Publish with confirm failed, buffering message", "Producer", { exchange: this.exchangeName, err });
      return this.bufferMessage(data, options, true);
    }
  }

  private bufferMessage(data: T, options: PublishOptions, confirm: boolean): Promise<boolean> {
    if (this.buffer.length >= this.maxBufferSize) {
      this.logger.error("Buffer full, dropping message", "Producer", { maxBufferSize: this.maxBufferSize, exchange: this.exchangeName });
      return Promise.reject(new Error(`[Producer] Buffer full (${this.maxBufferSize}). Message dropped.`));
    }
    return new Promise((resolve, reject) => {
      this.buffer.push({ data, options, resolve, reject, confirm });
      this.logger.warn("Message buffered", "Producer", { buffered: this.buffer.length, max: this.maxBufferSize, exchange: this.exchangeName });
    });
  }

  public getBufferSize(): number {
    return this.buffer.length;
  }
}
