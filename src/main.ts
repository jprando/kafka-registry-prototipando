import broker from "./broker";
import { executarReceberMensagem } from "./consumer";
import { executarEnviarMensagem } from "./producer";

const ehParaExecutar = process.argv.at(-1);

console.info({ ehParaExecutar });

switch (ehParaExecutar) {
	case "enviarMensagem":
		const producers = Array.from({ length: 50 }, () =>
			executarEnviarMensagem(broker),
		);
		const interval = setInterval(() => {
			const quantidadeProducer = producers.reduce(
				(acc: number, p: any) => acc + p.quantidade,
				0,
			);
			process.stdout.write(`#INFO ${quantidadeProducer} PRODUCER.SEND...\n`);
			producers.forEach((p: any) => (p.quantidade = 0));
		}, 5000);
		process.on("SIGINT", () => clearInterval(interval));
		process.on("SIGILL", () => clearInterval(interval));
		process.on("SIGTERM", () => clearInterval(interval));
		break;
	case "receberMensagem":
		executarReceberMensagem(broker);
		break;
	default:
		console.error("opcao nao conhecida, saindo...");
}
