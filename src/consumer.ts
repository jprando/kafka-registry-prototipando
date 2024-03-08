import { Consumer, Kafka } from "kafkajs";
import registry from "./registry";
import topic from "./topic";

async function receberMensagem(consumer: Consumer) {
  (consumer as any).quantidade = 0;
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
        (consumer as any).quantidade += 1;
      },
    });
  } catch (e) {
    if (e instanceof Error) {
      console.error("#ERRO", `${e.name}:`, e.message);
    }
    consumer.stop();
    consumer.disconnect();
    await new Promise((resolve) => setTimeout(resolve, 2500));
  }
}

export function executarReceberMensagem(broker: Kafka) {
  const consumers = Array.from({ length: 3 }, () =>
    broker.consumer({ groupId: "teste-group-ABC" }),
  );
  for (const consumer of consumers) {
    process.on("SIGINT", consumer.disconnect);
    process.on("SIGILL", consumer.disconnect);
    process.on("SIGTERM", consumer.disconnect);
    receberMensagem(consumer);
  }
  const interval = setInterval(() => {
    const quantidadeConsumer = consumers.reduce((acc: number, c: any) => {
      acc += (c as any).quantidade;
      (c as any).quantidade = 0;
      return acc;
    }, 0);
    process.stdout.write(`#INFO ${quantidadeConsumer} CONSUMER.RUN...\n`);
  }, 5000);
  process.on("SIGINT", () => clearInterval(interval));
  process.on("SIGILL", () => clearInterval(interval));
  process.on("SIGTERM", () => clearInterval(interval));
  return consumers;
}
