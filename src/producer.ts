import type { Kafka, Producer } from "kafkajs";
import { CompressionTypes, default as kafka } from "kafkajs";
import SnappyCodec from "kafkajs-snappy";
import { topic } from "./app";
import registry, { obterSchemaId } from "./registry";
import { somarQuantidades } from "./utils";

kafka.CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

const _tipo = ["temperatura", "humidade", "presenca", "rastreamento-veicular"];
const controle = new AbortController();

async function enviarMensagem(producer: Producer & { quantidade: number }) {
  try {
    await producer.connect();
    producer.on("producer.disconnect", () => {
      controle.abort();
    });
    const testeKeyRegistryId = await obterSchemaId("teste-key");
    const testeValueRegistryId = await obterSchemaId("teste-value");
    while (!controle.signal.aborted) {
      const tipo = _tipo[Number((Math.random() * 3).toFixed(0))];
      const key = await registry.encode(testeKeyRegistryId, {
        dispositivo: (Math.random() * 2000).toFixed(0),
      });
      const value = await registry.encode(testeValueRegistryId, {
        tipo,
        valor: Math.random(),
      });
      if (controle.signal.aborted) break;
      await producer.send({
        topic,
        compression: CompressionTypes.Snappy,
        messages: [
          {
            headers: {
              tipo,
              producao: "nao",
              teste: "sim",
            },
            key,
            value,
          },
        ],
      });
      producer.quantidade += 1;
    }
  } catch (e) {
    if (e instanceof Error) {
      console.error("#ERRO", `${e.name}:`, e.message);
    }
  }
}

export function executarEnviarMensagem(broker: Kafka) {
  const producers = Array.from({ length: 150 }, () => {
    const producer = broker.producer() as Producer & { quantidade: number };
    producer.quantidade = 0;
    enviarMensagem(producer);
    return producer;
  });

  const interval = setInterval(() => {
    const total = producers.reduce(somarQuantidades, 0);
    process.stdout.write(`#INFO ${(total / 5).toFixed(1)} SEND/s...\n`);
  }, 5000);

  async function finalizar() {
    clearInterval(interval);
    controle.abort();
    setTimeout(process.exit, 10000);
    await Promise.all(
      producers.map(async (producer) => {
        await producer.disconnect();
      }),
    );
    process.exit();
  }

  process.once("SIGINT", finalizar);
  process.once("SIGILL", finalizar);
  process.once("SIGTERM", finalizar);

  return producers;
}
