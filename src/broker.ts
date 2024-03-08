import assert from "assert";
import { Kafka, type Producer } from "kafkajs";

const { BROKER1, BROKER2, BROKER3 } = process.env;
assert(BROKER1, "BROKER1 nao esta configurado");
assert(BROKER2, "BROKER2 nao esta configurado");
assert(BROKER3, "BROKER3 nao esta configurado");

export default new Kafka({
  clientId: `teste-${Date.now()}`,
  brokers: [BROKER1, BROKER2, BROKER3],
  retry: { retries: 10 },
});

console.info("[broker] OK conectado");
