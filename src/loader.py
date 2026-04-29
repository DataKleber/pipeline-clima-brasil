import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def criar_conexao():
    """Cria conexao com o banco de dados"""
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    url = f"postgresql://{user}:{password}@{host}:{port}/{name}"
    engine = create_engine(url)
    print("Conexao com banco criada!")
    return engine

def criar_tabela(engine):
    """Cria a tabela se nao existir"""
    sql = """
    CREATE TABLE IF NOT EXISTS clima_cidades (
        id SERIAL PRIMARY KEY,
        cidade VARCHAR(100),
        temperatura_c FLOAT,
        sensacao_termica_c FLOAT,
        umidade_pct FLOAT,
        descricao VARCHAR(200),
        vento_kmh FLOAT,
        extraido_em TIMESTAMP,
        processado_em TIMESTAMP
    );
    """
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    print("Tabela criada com sucesso!")

def carregar(df, engine):
    """Carrega os dados no banco"""
    df.to_sql(
        name="clima_cidades",
        con=engine,
        if_exists="append",
        index=False
    )
    print(f"{len(df)} registros salvos no banco!")

# Teste rapido
if __name__ == "__main__":
    from extractor import extrair_todos
    from transformer import transformar

    resultados = extrair_todos()
    df = transformar(resultados)
    engine = criar_conexao()
    criar_tabela(engine)
    carregar(df, engine)
    print("Pipeline completo!")