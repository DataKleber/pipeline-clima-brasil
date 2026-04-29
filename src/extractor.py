import requests
from datetime import datetime

# Cidades brasileiras que vamos monitorar
CIDADES = [
    "Sao Paulo,BR",
    "Rio de Janeiro,BR",
    "Brasilia,BR",
    "Salvador,BR",
    "Fortaleza,BR"
]

BASE_URL = "https://wttr.in"

def buscar_clima(cidade):
    """Busca dados de clima de uma cidade"""
    try:
        url = f"{BASE_URL}/{cidade}?format=j1"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        dados = response.json()
        print(f"Dados obtidos para: {cidade}")
        return dados
    except Exception as e:
        print(f"Erro ao buscar {cidade}: {e}")
        return None

def extrair_todos():
    """Extrai dados de todas as cidades"""
    print("Iniciando extracao de dados...")
    resultados = []
    for cidade in CIDADES:
        dados = buscar_clima(cidade)
        if dados:
            resultados.append({
                "cidade": cidade.split(",")[0],
                "dados_brutos": dados,
                "extraido_em": datetime.now().isoformat()
            })
    print(f"Extracao concluida! {len(resultados)} cidades coletadas.")
    return resultados

# Teste rapido
if __name__ == "__main__":
    dados = extrair_todos()
    print(dados[0]["cidade"])