import type amqp from "amqplib";
import { RabbitLogger } from "../logger/logger.js";

export class ChannelManager {
  private static logger: RabbitLogger = new RabbitLogger();

  public static addLogger(logger: RabbitLogger) {
    ChannelManager.logger = logger;
  }

  public static async createChannel(conn: amqp.ChannelModel, onCloseCb: () => void): Promise<amqp.Channel> {
    const channel = await conn.createChannel();
    channel.on("error", (err) => ChannelManager.logger.error("Channel error", "ChannelManager", { err }));
    channel.on("close", onCloseCb);
    return channel;
  }

  public static async createConfirmChannel(conn: amqp.ChannelModel, onCloseCb: () => void): Promise<amqp.ConfirmChannel> {
    const channel = await conn.createConfirmChannel();
    channel.on("error", (err) => ChannelManager.logger.error("Confirm channel error", "ChannelManager", { err }));
    channel.on("close", onCloseCb);
    return channel;
  }

  public static async closeChannels(chans: amqp.Channel[], confirmChans: amqp.ConfirmChannel[]) {
    const errors: unknown[] = [];
    for (const ch of [...chans, ...confirmChans]) {
      try {
        await ch.close();
      } catch (err) {
        errors.push(err);
      }
    }
    if (errors.length > 0)
      throw new AggregateError(errors, "[ChannelManager] Some channels failed to close");
  }
}
