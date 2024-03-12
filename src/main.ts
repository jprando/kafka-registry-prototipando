import broker from "./broker";
import { executarReceberMensagem } from "./consumer";
import { executarEnviarMensagem } from "./producer";

const ehParaExecutar = process.argv.at(-1);

console.info({ ehParaExecutar });

switch (ehParaExecutar) {
  case "enviarMensagem": {
    const producers = executarEnviarMensagem(broker);
    console.info(producers.length, "producers criados...");
    break;
  }
  case "receberMensagem": {
    const consumers = executarReceberMensagem(broker);
    console.info(consumers.length, "consumers criados...");
    break;
  }
  default:
    console.error("opcao nao conhecida, saindo...");
}
