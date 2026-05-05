# Imagem base com Python 3.11
FROM python:3.11-slim

# Diretorio de trabalho dentro do container
WORKDIR /app

# Copia o arquivo de dependencias primeiro
COPY requirements.txt .

# Instala as dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o projeto
COPY . .

# Comando para rodar o pipeline
CMD ["python", "src/main.py"]