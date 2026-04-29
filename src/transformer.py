import pandas as pd
from datetime import datetime

def transformar(resultados):
    """Transforma dados brutos em formato estruturado"""
    print("Iniciando transformacao dos dados...")
    registros = []

    for item in resultados:
        cidade = item["cidade"]
        dados = item["dados_brutos"]
        extraido_em = item["extraido_em"]

        try:
            condicao = dados["current_condition"][0]

            registro = {
                "cidade": cidade,
                "temperatura_c": float(condicao["temp_C"]),
                "sensacao_termica_c": float(condicao["FeelsLikeC"]),
                "umidade_pct": float(condicao["humidity"]),
                "descricao": condicao["weatherDesc"][0]["value"],
                "vento_kmh": float(condicao["windspeedKmph"]),
                "extraido_em": extraido_em,
                "processado_em": datetime.now().isoformat()
            }
            registros.append(registro)
            print(f"{cidade}: {registro['temperatura_c']}C, {registro['descricao']}")

        except Exception as e:
            print(f"Erro ao transformar {cidade}: {e}")

    df = pd.DataFrame(registros)
    print(f"Transformacao concluida! {len(df)} registros processados.")
    return df

# Teste rapido
if __name__ == "__main__":
    from extractor import extrair_todos
    resultados = extrair_todos()
    df = transformar(resultados)
    print("\n--- Dados transformados ---")
    print(df.to_string())