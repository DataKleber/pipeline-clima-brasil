import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
 
load_dotenv()
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
 
 
def criar_conexao():
    """Cria e retorna engine de conexao com o banco PostgreSQL"""
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
 
    url = f"postgresql://{user}:{password}@{host}:{port}/{name}"
    engine = create_engine(url)
    logging.info("Conexao com banco criada!")
    return engine
 
 
def criar_tabela(engine):
    """
    Cria tabelas Silver e Gold se nao existirem.
    - Silver: dados limpos com constraint UNIQUE (cidade + extraido_em)
    - Gold: agregacoes analiticas por cidade
    """
    sql_silver = """
    CREATE TABLE IF NOT EXISTS clima_cidades (
        id              SERIAL PRIMARY KEY,
        cidade          VARCHAR(100)  NOT NULL,
        temperatura_c   FLOAT,
        sensacao_termica_c FLOAT,
        umidade_pct     FLOAT,
        descricao       VARCHAR(200),
        vento_kmh       FLOAT,
        extraido_em     TIMESTAMP     NOT NULL,
        processado_em   TIMESTAMP,
        source          VARCHAR(100),
        ingestion_time  TIMESTAMP,
        CONSTRAINT uq_cidade_extraido UNIQUE (cidade, extraido_em)
    );
    """
 
    sql_gold = """
    CREATE TABLE IF NOT EXISTS clima_analitico (
        id              SERIAL PRIMARY KEY,
        cidade          VARCHAR(100)  NOT NULL,
        temp_media_c    FLOAT,
        temp_max_c      FLOAT,
        temp_min_c      FLOAT,
        umidade_media   FLOAT,
        total_registros INTEGER,
        atualizado_em   TIMESTAMP
    );
    """
 
    with engine.connect() as conn:
        conn.execute(text(sql_silver))
        conn.execute(text(sql_gold))
        conn.commit()
    logging.info("Tabelas Silver e Gold verificadas/criadas com sucesso!")
 
 
def _filtrar_incremental(df: pd.DataFrame, engine) -> pd.DataFrame:
    """
    Incremental Load: remove do DataFrame registros que ja existem no banco.
    Usa a combinacao (cidade + extraido_em) como chave de deduplicacao.
    """
    try:
        existentes = pd.read_sql(
            "SELECT cidade, extraido_em FROM clima_cidades", engine
        )
        if existentes.empty:
            logging.info("Banco vazio. Todos os registros serao inseridos.")
            return df
 
        existentes["extraido_em"] = pd.to_datetime(existentes["extraido_em"])
        df["extraido_em"] = pd.to_datetime(df["extraido_em"])
 
        # Cria chave composta para comparacao
        chaves_existentes = set(
            zip(existentes["cidade"], existentes["extraido_em"])
        )
        mascara_nova = df.apply(
            lambda row: (row["cidade"], row["extraido_em"]) not in chaves_existentes,
            axis=1
        )
        df_novo = df[mascara_nova]
        duplicatas = len(df) - len(df_novo)
 
        if duplicatas > 0:
            logging.warning(f"Incremental Load: {duplicatas} registro(s) ja existiam — ignorados.")
        logging.info(f"Incremental Load: {len(df_novo)} registro(s) novos para inserir.")
        return df_novo
 
    except Exception as e:
        logging.error(f"Erro ao verificar registros existentes: {e}")
        return df
 
 
def _atualizar_gold(engine):
    """
    Camada Gold: recalcula e substitui agregacoes por cidade
    na tabela clima_analitico.
    """
    sql = """
    DELETE FROM clima_analitico;
 
    INSERT INTO clima_analitico (
        cidade, temp_media_c, temp_max_c, temp_min_c,
        umidade_media, total_registros, atualizado_em
    )
    SELECT
        cidade,
        ROUND(AVG(temperatura_c)::numeric, 2)   AS temp_media_c,
        MAX(temperatura_c)                       AS temp_max_c,
        MIN(temperatura_c)                       AS temp_min_c,
        ROUND(AVG(umidade_pct)::numeric, 2)      AS umidade_media,
        COUNT(*)                                 AS total_registros,
        NOW()                                    AS atualizado_em
    FROM clima_cidades
    GROUP BY cidade;
    """
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    logging.info("Camada Gold (clima_analitico) atualizada com sucesso!")
 
 
def carregar(df: pd.DataFrame, engine):
    """
    Carrega dados Silver no banco com Incremental Load.
    Apos a carga, atualiza a camada Gold.
    """
    if df.empty:
        logging.warning("DataFrame vazio. Nada para carregar.")
        return
 
    df_novo = _filtrar_incremental(df, engine)
 
    if df_novo.empty:
        logging.info("Nenhum registro novo para inserir. Pipeline encerrado.")
        _atualizar_gold(engine)
        return
 
    try:
        df_novo.to_sql(
            name="clima_cidades",
            con=engine,
            if_exists="append",
            index=False,
            method="multi"
        )
        logging.info(f"{len(df_novo)} registro(s) inseridos na tabela Silver (clima_cidades).")
    except Exception as e:
        logging.error(f"Erro ao inserir dados no banco: {e}")
        raise
 
    _atualizar_gold(engine)
 
 
if __name__ == "__main__":
    import sys, os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'bronze'))
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'silver'))
    from extractor import extrair_todos
    from transformer import transformar

    resultados = extrair_todos()
    df = transformar(resultados)
    engine = criar_conexao()
    criar_tabela(engine)
    carregar(df, engine)
    logging.info("Teste do loader concluido!")