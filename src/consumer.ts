import type { Consumer, Kafka } from "kafkajs";
import registry from "./registry";
import topic from "./topic";
import { somarQuantidades } from "./utils";

async function receberMensagem(consumer: Consumer & { quantidade: number }) {
  await consumer.connect();
  await consumer.subscribe({ topic });
  try {
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const tipo = message.headers?.tipo?.toString();
        const key = message.key
          ? await registry.decode(message.key)
          : undefined;
        const mensagem = message.value
          ? await registry.decode(message.value)
          : undefined;
        const data = { topic, partition, tipo, key, mensagem };
        consumer.quantidade += 1;
      },
    });
  } catch (e) {
    if (e instanceof Error) {
      console.error("#ERRO", `${e.name}:`, e.message);
    }
  }
}

export function executarReceberMensagem(broker: Kafka) {
  const consumers = Array.from({ length: 3 }, () => {
    const consumer = broker.consumer({
      groupId: "teste-group-ABC",
    }) as Consumer & { quantidade: number };
    consumer.quantidade = 0;
    receberMensagem(consumer);
    return consumer;
  });

  const interval = setInterval(() => {
    const total = consumers.reduce(somarQuantidades, 0);
    process.stdout.write(`#INFO ${(total / 5).toFixed(1)} RUN/s...\n`);
  }, 5000);

  async function finalizar() {
    clearInterval(interval);
    setTimeout(process.exit, 10000);
    await Promise.all(
      consumers.map(async (consumer) => {
        await consumer.stop();
        await consumer.disconnect();
      }),
    );
    process.exit();
  }

  process.once("SIGINT", finalizar);
  process.once("SIGILL", finalizar);
  process.once("SIGTERM", finalizar);

  return consumers;
}
