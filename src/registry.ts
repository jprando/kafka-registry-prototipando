import assert from "node:assert";
import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";

const { REGISTRY } = process.env;
assert(REGISTRY, "REGISTRY nao esta configurado");

export default new SchemaRegistry({
  host: REGISTRY,
});
