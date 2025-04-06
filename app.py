from flask import Flask, jsonify
import requests
from datetime import datetime
import os
import ijson

app = Flask(__name__)

MAX_ITENS = 500  # Limite de registros processados

# Função para conversão de timestamp
def timestamp_para_datetime(ms_timestamp):
    try:
        return datetime.fromtimestamp(int(ms_timestamp) / 1000)
    except:
        return None

# Função para normalizar coordenadas (string para float)
def converter_coordenada(coord_str):
    try:
        return float(coord_str.replace(",", "."))
    except:
        return None

# Função que trata um registro bruto da API
def tratar_registro(dado_bruto):
    try:
        latitude = converter_coordenada(dado_bruto.get("latitude", "0"))
        longitude = converter_coordenada(dado_bruto.get("longitude", "0"))
        velocidade = float(dado_bruto.get("velocidade", 0))

        datahora = timestamp_para_datetime(dado_bruto.get("datahora"))
        datahoraserv = timestamp_para_datetime(dado_bruto.get("datahoraservidor"))

        return {
            "ordem": dado_bruto.get("ordem"),
            "linha": dado_bruto.get("linha"),
            "latitude": latitude,
            "longitude": longitude,
            "velocidade": velocidade,
            "datahora": datahora,
            "datahoraservidor": datahoraserv,
            "atraso_ms": int(dado_bruto.get("datahoraservidor", 0)) - int(dado_bruto.get("datahora", 0))
        }
    except Exception as e:
        print(f"[ERRO] Falha ao tratar dado: {e}")
        return None

# Endpoint principal da API
@app.route("/", methods=["GET"])
def home():
    return "API - Coleta de ônibus RJ 🚍"

# Endpoint de dados brutos (sem tratamento)
@app.route("/onibus_bruto", methods=["GET"])
def onibus_bruto():
    return coletar_dados(tratamento=False)

# Endpoint de dados tratados (com filtros e transformação)
@app.route("/onibus_tratado", methods=["GET"])
def onibus_tratado():
    return coletar_dados(tratamento=True)

# Função que coleta da API da prefeitura e retorna o JSON
def coletar_dados(tratamento=False):
    url_api = "https://dados.mobilidade.rio/gps/sppo"
    try:
        resposta = requests.get(url_api, stream=True, timeout=30)
        resposta.raise_for_status()
        itens = ijson.items(resposta.raw, 'item')

        registros = []
        for item in itens:
            if not item:
                continue

            if tratamento:
                if float(item.get("velocidade", 0)) <= 0:
                    continue
                tratado = tratar_registro(item)
                if tratado:
                    registros.append(tratado)
            else:
                registros.append(item)

            if len(registros) >= MAX_ITENS:
                break

        registros.sort(key=lambda x: x["datahora"] if tratamento else x.get("datahora", ""), reverse=True)

        # Formatar datas para string apenas no endpoint tratado
        if tratamento:
            for r in registros:
                r["datahora"] = r["datahora"].strftime("%Y-%m-%d %H:%M:%S") if r["datahora"] else None
                r["datahoraservidor"] = r["datahoraservidor"].strftime("%Y-%m-%d %H:%M:%S") if r["datahoraservidor"] else None

        return jsonify(registros)

    except Exception as e:
        print(f"[ERRO] Coleta falhou: {str(e)}")
        return jsonify({"erro": f"Falha na coleta: {str(e)}"}), 500

# Rodar o app Flask
if __name__ == "__main__":
    porta = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=porta)
