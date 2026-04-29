from extractor import extrair_todos
from transformer import transformar
from loader import criar_conexao, criar_tabela, carregar

def rodar_pipeline():
    print("=== PIPELINE CLIMA BRASIL ===")
    print("")
    
    # Extract
    resultados = extrair_todos()
    
    # Transform
    df = transformar(resultados)
    
    # Load
    engine = criar_conexao()
    criar_tabela(engine)
    carregar(df, engine)
    
    print("")
    print("=== PIPELINE FINALIZADO COM SUCESSO ===")

if __name__ == "__main__":
    rodar_pipeline()