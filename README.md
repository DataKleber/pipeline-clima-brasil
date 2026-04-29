# Pipeline Clima Brasil

Pipeline ETL que coleta dados de clima em tempo real de 5 cidades brasileiras, transforma e armazena em banco de dados PostgreSQL.

## Tecnologias
- Python
- PostgreSQL
- pandas
- SQLAlchemy
- API wttr.in

## Como executar

1. Clone o repositorio
2. Crie o ambiente virtual e ative
3. Instale as dependencias: pip install -r requirements.txt
4. Configure o arquivo .env com suas credenciais do banco
5. Execute: python src/main.py

## Resultado
Dados armazenados no PostgreSQL com temperatura, sensacao termica, umidade, vento e descricao do clima.

## Autor
Kleber Xavier — linkedin.com/in/kleber-xavier-de-carvalho