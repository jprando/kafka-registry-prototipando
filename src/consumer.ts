import { Consumer, Kafka } from "kafkajs";
import registry from "./registry";
import topic from "./topic";

async function receberMensagem(consumer: Consumer) {
	(consumer as any).quantidade = 0;
	await consumer.connect();
	await consumer.subscribe({ topic });
	try {
		const interval = setInterval(() => {
			process.stdout.write(
				`#INFO ${(consumer as any).quantidade} CONSUMER.RUN...\n`,
			);
			(consumer as any).quantidade = 0;
		}, 5000);
		process.on("SIGINT", () => clearInterval(interval));
		process.on("SIGILL", () => clearInterval(interval));
		process.on("SIGTERM", () => clearInterval(interval));
		await consumer.run({
			eachMessage: async ({ topic, partition, message }) => {
				const data = {
					data: new Date().toISOString(),
					partition,
					tipo: message.headers?.tipo?.toString(),
					key: message.key && (await registry.decode(message.key)),
					mensagem: message.value && (await registry.decode(message.value)),
				};
				(consumer as any).quantidade += 1;
			},
		});
	} catch (e) {
		// await consumer.stop();
		await consumer.disconnect();
		if (e instanceof Error) {
			console.error("#ERRO", `${e.name}:`, e.message);
		}
	}
}

export function executarReceberMensagem(broker: Kafka) {
	const consumers = Array.from({ length: 1 }, () =>
		broker.consumer({ groupId: "teste-group-ABC" }),
	);
	for (const consumer of consumers) {
		const stopDisconnectConsumer = async () => {
			// await consumer.stop();
			// await consumer.disconnect();
			consumer.stop();
			consumer.disconnect();
			await new Promise((resolve) => setTimeout(resolve, 2500));
		};
		process.on("SIGINT", stopDisconnectConsumer);
		process.on("SIGILL", stopDisconnectConsumer);
		process.on("SIGTERM", stopDisconnectConsumer);
		receberMensagem(consumer);
	}
}
