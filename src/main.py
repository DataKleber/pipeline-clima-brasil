import sys
import os
import logging
 
# Configuracao central de logging (deve ser feita antes dos imports dos modulos)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),                  # console
        logging.FileHandler("pipeline_clima.log", mode="a") # arquivo de log
    ]
)
 
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'bronze'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'silver'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'gold'))
 
from extractor import extrair_todos
from transformer import transformar
from loader import criar_conexao, criar_tabela, carregar
 
 
def rodar_pipeline():
    logging.info("=" * 50)
    logging.info("PIPELINE CLIMA BRASIL — Iniciando")
    logging.info("Arquitetura: Bronze -> Silver -> Gold")
    logging.info("=" * 50)
 
    # BRONZE — Extracao de dados brutos
    logging.info("--- BRONZE: Extraindo dados brutos ---")
    try:
        resultados = extrair_todos()
    except Exception as e:
        logging.error(f"Falha critica na extracao (Bronze): {e}")
        sys.exit(1)
 
    if not resultados:
        logging.warning("Nenhum dado extraido. Encerrando pipeline.")
        return
 
    # SILVER — Transformacao e Data Quality
    logging.info("--- SILVER: Transformando e validando dados ---")
    try:
        df = transformar(resultados)
    except Exception as e:
        logging.error(f"Falha critica na transformacao (Silver): {e}")
        sys.exit(1)
 
    if df.empty:
        logging.warning("DataFrame vazio apos transformacao. Encerrando pipeline.")
        return
 
    # GOLD — Carga incremental + agregacoes
    logging.info("--- GOLD: Carregando no banco + atualizando agregacoes ---")
    try:
        engine = criar_conexao()
        criar_tabela(engine)
        carregar(df, engine)
    except Exception as e:
        logging.error(f"Falha critica na carga (Gold): {e}")
        sys.exit(1)
 
    logging.info("=" * 50)
    logging.info("PIPELINE FINALIZADO COM SUCESSO")
    logging.info("=" * 50)
 
 
if __name__ == "__main__":
    rodar_pipeline()