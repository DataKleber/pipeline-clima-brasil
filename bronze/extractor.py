import requests
import logging
from datetime import datetime
 
# Configuracao de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
 
CIDADES = [
    "Sao Paulo,BR",
    "Rio de Janeiro,BR",
    "Brasilia,BR",
    "Salvador,BR",
    "Fortaleza,BR"
]
 
BASE_URL = "https://wttr.in"
 
 
def buscar_clima(cidade: str) -> dict | None:
    """Busca dados de clima de uma cidade via API wttr.in"""
    try:
        url = f"{BASE_URL}/{cidade}?format=j1"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        dados = response.json()
        logging.info(f"Dados obtidos para: {cidade}")
        return dados
    except requests.exceptions.Timeout:
        logging.error(f"Timeout ao buscar {cidade}")
        return None
    except requests.exceptions.HTTPError as e:
        logging.error(f"Erro HTTP ao buscar {cidade}: {e}")
        return None
    except Exception as e:
        logging.error(f"Erro inesperado ao buscar {cidade}: {e}")
        return None
 
 
def extrair_todos() -> list[dict]:
    """Extrai dados de todas as cidades e retorna lista de registros brutos"""
    logging.info("Iniciando extracao de dados (Bronze)...")
    resultados = []
 
    for cidade in CIDADES:
        dados = buscar_clima(cidade)
        if dados:
            resultados.append({
                "cidade": cidade.split(",")[0],
                "dados_brutos": dados,
                "extraido_em": datetime.now(),   # datetime nativo, nao string
                "source": "wttr_api"
            })
 
    logging.info(f"Extracao concluida! {len(resultados)}/{len(CIDADES)} cidades coletadas.")
    return resultados
 
 
if __name__ == "__main__":
    dados = extrair_todos()
    if dados:
        logging.info(f"Primeira cidade extraida: {dados[0]['cidade']}")