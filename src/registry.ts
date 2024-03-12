import assert from "node:assert";
import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";

const { REGISTRY } = process.env;
assert(REGISTRY, "REGISTRY nao esta configurado");

const registry = new SchemaRegistry({
  host: REGISTRY,
});

export default registry;

const cache = {
  "teste-key": 0,
  "teste-value": 0,
};

export async function obterSchemaId(nomeSchema: keyof typeof cache) {
  if (!cache[nomeSchema]) {
    cache[nomeSchema] = await registry.getLatestSchemaId(nomeSchema);
  }
  return cache[nomeSchema];
}
