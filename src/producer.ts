import avro from "avsc";
import { CompressionTypes, Kafka, Producer, default as kafka } from "kafkajs";
import SnappyCodec from "kafkajs-snappy";
import registry from "./registry";
import topic from "./topic";

kafka.CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

async function enviarMensagem(producer: Producer) {
	(producer as any).quantidade = 0;
	try {
		await producer.connect();
		let continuar = true;
		producer.on("producer.disconnect", () => {
			continuar = false;
		});
		while (continuar) {
			const key = await registry.encode(5, {
				dispositivo: (Math.random() * 2000).toFixed(0),
			});
			const value = await registry.encode(6, {
				valor: Math.random(),
			});
			await producer.send({
				topic,
				compression: CompressionTypes.Snappy,
				messages: [
					{
						headers: {
							tipo: [
								"temperatura",
								"humidade",
								"presenca",
								"rastreamento-veicular",
							][Number((Math.random() * 3).toFixed(0))],
							producao: "nao",
							teste: "sim",
						},
						key,
						value,
					},
				],
			});
			(producer as any).quantidade += 1;
			// process.stdout.write(".");
		}
	} catch (e) {
		await producer.disconnect();
		if (e instanceof Error) {
			console.error("#ERRO", `${e.name}:`, e.message);
		}
	}
}

export function executarEnviarMensagem(broker: Kafka) {
	const producer = broker.producer();

	process.on("SIGINT", producer.disconnect);
	process.on("SIGILL", producer.disconnect);
	process.on("SIGTERM", producer.disconnect);

	enviarMensagem(producer);
	return producer;
}
