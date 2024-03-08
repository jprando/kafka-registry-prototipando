import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";

export default new SchemaRegistry({
  host: "http://broker.prando.dev.br:18081/",
});
