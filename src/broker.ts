import assert from "node:assert";
import chalk from "chalk";
import { Kafka } from "kafkajs";
import { clientId } from "./app";

const { BROKER1, BROKER2, BROKER3 } = process.env;
assert(BROKER1, "BROKER1 nao esta configurado");
assert(BROKER2, "BROKER2 nao esta configurado");
assert(BROKER3, "BROKER3 nao esta configurado");

export default new Kafka({
  clientId,
  brokers: [BROKER1, BROKER2, BROKER3],
  retry: { retries: 10 },
});

console.info(
  chalk.green("[BROKER]"),
  chalk.yellowBright("OK"),
  chalk.whiteBright("conectado"),
  chalk.dim(clientId),
);
