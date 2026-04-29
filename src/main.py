import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'bronze'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'silver'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'gold'))

from extractor import extrair_todos
from transformer import transformar
from loader import criar_conexao, criar_tabela, carregar

def rodar_pipeline():
    print("=== PIPELINE CLIMA BRASIL ===")
    print("Arquitetura: Bronze -> Silver -> Gold")
    print("")

    # BRONZE - Dados brutos
    print("--- BRONZE: Extraindo dados brutos ---")
    resultados = extrair_todos()

    # SILVER - Dados limpos
    print("--- SILVER: Transformando dados ---")
    df = transformar(resultados)

    # GOLD - Dados prontos para analise
    print("--- GOLD: Carregando no banco ---")
    engine = criar_conexao()
    criar_tabela(engine)
    carregar(df, engine)

    print("")
    print("=== PIPELINE FINALIZADO COM SUCESSO ===")

if __name__ == "__main__":
    rodar_pipeline()