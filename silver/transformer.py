import pandas as pd
import logging
from datetime import datetime
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
 
# Limites para remocao de outliers por coluna (IQR aplicado depois)
COLUNAS_NUMERICAS = ["temperatura_c", "sensacao_termica_c", "umidade_pct", "vento_kmh"]
 
COLUNAS_OBRIGATORIAS = [
    "cidade", "temperatura_c", "sensacao_termica_c",
    "umidade_pct", "descricao", "vento_kmh",
    "extraido_em", "processado_em", "source", "ingestion_time"
]
 
 
def _remover_outliers_iqr(df: pd.DataFrame, colunas: list[str]) -> pd.DataFrame:
    """Remove linhas com outliers usando o metodo IQR para cada coluna numerica"""
    df_limpo = df.copy()
    total_antes = len(df_limpo)
 
    for col in colunas:
        if col not in df_limpo.columns:
            continue
        Q1 = df_limpo[col].quantile(0.25)
        Q3 = df_limpo[col].quantile(0.75)
        IQR = Q3 - Q1
        limite_inf = Q1 - 1.5 * IQR
        limite_sup = Q3 + 1.5 * IQR
        antes = len(df_limpo)
        df_limpo = df_limpo[(df_limpo[col] >= limite_inf) & (df_limpo[col] <= limite_sup)]
        removidos = antes - len(df_limpo)
        if removidos > 0:
            logging.warning(f"Outliers removidos em '{col}': {removidos} registro(s)")
 
    logging.info(f"IQR: {total_antes - len(df_limpo)} registro(s) removidos no total")
    return df_limpo
 
 
def _garantir_qualidade(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica regras de data quality: tipos, nulos e outliers"""
    tamanho_original = len(df)
 
    # Forcar tipos numericos (converte erros para NaN)
    for col in COLUNAS_NUMERICAS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
 
    # Garantir que extraido_em e datetime (nao string)
    df["extraido_em"] = pd.to_datetime(df["extraido_em"], errors="coerce")
 
    # Remover linhas com nulos em colunas criticas
    df = df.dropna(subset=["cidade", "temperatura_c", "umidade_pct", "extraido_em"])
 
    nulos_removidos = tamanho_original - len(df)
    if nulos_removidos > 0:
        logging.warning(f"Registros removidos por nulos: {nulos_removidos}")
 
    # Remover outliers via IQR
    if len(df) > 1:
        df = _remover_outliers_iqr(df, COLUNAS_NUMERICAS)
 
    return df
 
 
def transformar(resultados: list[dict]) -> pd.DataFrame:
    """Transforma dados brutos (Bronze) em dados estruturados e limpos (Silver)"""
    logging.info("Iniciando transformacao dos dados (Silver)...")
    registros = []
    agora = pd.Timestamp.now()
 
    for item in resultados:
        cidade = item["cidade"]
        dados = item["dados_brutos"]
        extraido_em = item["extraido_em"]
        source = item.get("source", "desconhecido")
 
        try:
            condicao = dados["current_condition"][0]
 
            registro = {
                "cidade": cidade,
                "temperatura_c": condicao.get("temp_C"),
                "sensacao_termica_c": condicao.get("FeelsLikeC"),
                "umidade_pct": condicao.get("humidity"),
                "descricao": condicao["weatherDesc"][0]["value"],
                "vento_kmh": condicao.get("windspeedKmph"),
                "extraido_em": extraido_em,          # datetime nativo
                "processado_em": datetime.now(),
                "source": source,                    # metadado de origem
                "ingestion_time": agora              # metadado de ingestao
            }
            registros.append(registro)
            logging.info(f"{cidade}: {registro['temperatura_c']}C | {registro['descricao']}")
 
        except (KeyError, IndexError, TypeError) as e:
            logging.error(f"Erro ao transformar {cidade}: {e}")
 
    if not registros:
        logging.warning("Nenhum registro transformado. Retornando DataFrame vazio.")
        return pd.DataFrame(columns=COLUNAS_OBRIGATORIAS)
 
    df = pd.DataFrame(registros)
 
    # Data Quality
    df = _garantir_qualidade(df)
 
    logging.info(f"Transformacao concluida! {len(df)} registro(s) prontos para carga.")
    return df
 
 
if __name__ == "__main__":
    import sys, os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'bronze'))
    from extractor import extrair_todos
    resultados = extrair_todos()
    df = transformar(resultados)
    print("\n--- Dados transformados ---")
    print(df.to_string())