import { Inject, Injectable, Logger } from '@nestjs/common';
import { ClientProxy, ReadPacket, WritePacket } from '@nestjs/microservices';
import { Client } from '@nestjs/microservices/external/nats-client.interface';
import {
  Codec,
  connect,
  JSONCodec,
  NatsConnection,
  RequestOptions,
} from 'nats';
import { NATS_JETSTREAM_OPTIONS } from './constants';
import { NatsJetStreamClientOptions } from './interfaces/nats-jetstream-client-options.interface';
import { NatsJetStreamRecord } from './utils/nats-jetstream-record.builder';
import { NatsRequestRecord } from './utils/nats-request-record-builder';

@Injectable()
export class NatsJetStreamClientProxy extends ClientProxy {
  private readonly logger = new Logger(NatsJetStreamClientProxy.name);

  private nc: NatsConnection | undefined;
  private codec: Codec<JSON>;

  constructor(
    @Inject(NATS_JETSTREAM_OPTIONS) private options: NatsJetStreamClientOptions,
  ) {
    super();
    this.codec = JSONCodec();
  }

  async connect(): Promise<NatsConnection | undefined> {
    try {
      if (!this.nc) {
        this.nc = await connect(this.options.connectionOptions);
        this.handleStatusUpdates(this.nc as Client);
        if (this.options.connectionOptions.connectedHook) {
          this.options.connectionOptions.connectedHook(this.nc);
        }
      }
      return this.nc;
    } catch (err) {
      this.logger.error(err);
      return;
    }
  }

  async close() {
    await this.nc?.drain();
    this.nc = undefined;
  }

  protected publish(
    packet: ReadPacket,
    callback: (packet: WritePacket) => void,
  ): () => void {
    if (!this.nc) {
      callback({ err: new Error('NATS connection is not established') });
      return () => null;
    }
    const subject = this.normalizePattern(packet.pattern);
    if (packet.data instanceof NatsRequestRecord) {
      const options: RequestOptions = packet.data.options;
      const payload = this.codec.encode(packet.data.payload);
      this.nc
        .request(subject, payload, options)
        .then((msg) => this.codec.decode(msg.data) as WritePacket)
        .then((packet) => callback(packet))
        .catch((err) => {
          callback({ err });
        });
      return () => null;
    } else {
      const payload = this.codec.encode(packet.data);
      this.nc
        .request(subject, payload)
        .then((msg) => this.codec.decode(msg.data) as WritePacket)
        .then((packet) => callback(packet))
        .catch((err) => {
          callback({ err });
        });
      return () => null;
    }
  }

  protected async dispatchEvent(
    packet: ReadPacket<NatsJetStreamRecord>,
  ): Promise<any> {
    if (!this.nc) {
      return;
    }
    const subject = this.normalizePattern(packet.pattern);
    const jetStreamOpts = this.options.jetStreamOption;
    const js = this.nc.jetstream(jetStreamOpts);
    try {
      if (packet.data instanceof NatsJetStreamRecord) {
        const payload = this.codec.encode(packet.data.payload);
        const options = packet.data.options;
        return await js.publish(subject, payload, options);
      } else {
        const payload = this.codec.encode(packet.data);
        return await js.publish(subject, payload);
      }
    } catch (err) {
      // Happens when server is not yet connected to NATS
      this.logger.error(err);
    }
  }

  private async handleStatusUpdates(client: Client) {
    for await (const status of client.status()) {
      switch (status.type) {
        case 'error':
        case 'disconnect':
          this.nc = undefined;
          break;

        case 'reconnect':
          this.logger.warn('Reconnect');
          break;

        default:
          break;
      }
    }
  }
}
