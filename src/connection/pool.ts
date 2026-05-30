import { RabbitSingleConnectionHandler } from "./single.js";
import { RabbitLogger } from "../logger/logger.js";
import type { RabbitConnectionOptions } from "../types.js";

export class RabbitConnectionPoolHandler {
  private connections: RabbitSingleConnectionHandler[] = [];
  private poolSize: number;
  private connString: string;
  private options: RabbitConnectionOptions;
  private logger: RabbitLogger;

  public constructor(connString: string, poolSize: number = 3, options: RabbitConnectionOptions = {}) {
    this.connString = connString;
    this.poolSize = poolSize;
    this.options = options;
    this.logger = new RabbitLogger();
  }

  public addLogger(logger: RabbitLogger) {
    this.logger = logger;
    for (const conn of this.connections) conn.addLogger(logger);
  }

  public async ConnectToService() {
    this.logger.info("Initializing connection pool", "ConnectionPool", { size: this.poolSize });
    for (let i = 0; i < this.poolSize; i++) {
      const conn = new RabbitSingleConnectionHandler(this.connString, this.options);
      conn.addLogger(this.logger);
      await conn.ConnectToService();
      this.connections.push(conn);
      this.logger.debug("Connection added to pool", "ConnectionPool", { index: i });
    }
    this.logger.info("Connection pool ready", "ConnectionPool", { size: this.poolSize });
  }

  public acquire(): RabbitSingleConnectionHandler {
    if (this.connections.length === 0)
      throw new Error("[ConnectionPool] No connections available");
    const conn = this.connections.find((c) => c.rabbitConnection !== null);
    if (!conn) throw new Error("[ConnectionPool] No active connections available");
    this.logger.debug("Connection acquired", "ConnectionPool");
    return conn;
  }

  public onReconnect(cb: () => Promise<void>) {
    for (const conn of this.connections) conn.onReconnect(cb);
  }

  public async gracefulShutdown() {
    this.logger.info("Shutting down connection pool", "ConnectionPool", { size: this.connections.length });
    for (const conn of this.connections) await conn.gracefulShutdown();
    this.connections = [];
    this.logger.info("Connection pool shut down", "ConnectionPool");
  }
}
