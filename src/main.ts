import broker from "./broker";
import { executarReceberMensagem } from "./consumer";
import { executarEnviarMensagem } from "./producer";

const ehParaExecutar = process.argv.at(-1);

console.info({ ehParaExecutar });

switch (ehParaExecutar) {
  case "enviarMensagem":
    executarEnviarMensagem(broker);
    break;
  case "receberMensagem":
    executarReceberMensagem(broker);
    break;
  default:
    console.error("opcao nao conhecida, saindo...");
}
