import urllib.parse
from dotenv import dotenv_values
from sqlalchemy import create_engine, inspect

def obter_colunas(nome_tabela, string_conexao):
    """Conecta no banco e extrai as colunas e tipos da tabela informada."""
    try:
        engine = create_engine(string_conexao)
        inspetor = inspect(engine)
        
        # Verifica se o usuário digitou no formato schema.tabela (ex: produtos.produtos)
        partes = nome_tabela.split('.')
        if len(partes) == 2:
            schema_nome = partes[0]
            tabela_nome = partes[1]
        else:
            schema_nome = None
            tabela_nome = nome_tabela

        # Inspeciona passando o schema separadamente (se existir)
        if not inspetor.has_table(tabela_nome, schema=schema_nome):
            return {"erro": f"A tabela '{nome_tabela}' não foi encontrada no banco."}
            
        colunas_info = inspetor.get_columns(tabela_nome, schema=schema_nome)
        tipos = {coluna['name']: str(coluna['type']) for coluna in colunas_info}
        
        engine.dispose()
        return tipos
    except Exception as e:
        return {"erro": f"Falha na conexão ou execução: {str(e)}"}

def main():
    nome_tabela = input("Digite o nome da tabela que deseja inspecionar: ").strip()
    
    if not nome_tabela:
        print("Erro: Você precisa digitar o nome de uma tabela.")
        return
        
    env_dict = dotenv_values(".env")
    
    if not env_dict:
        print("Erro: Arquivo .env não encontrado ou está vazio na pasta atual.")
        return
    
    # O .strip() limpa espaços/quebras de linha nas pontas. 
    # O quote_plus protege contra caracteres especiais (@, #, !).
    # Lê e aplica URL-encoding em usuário, senha e nome do banco
    user_raw = env_dict.get('postgresql_username', '').strip()
    password_raw = env_dict.get('postgresql_password', '').strip()
    
    host = env_dict.get('postgresql_host', '').strip()
    port = env_dict.get('postgresql_port', '').strip()
    dbname_raw = env_dict.get('postgresql_name', '').strip()

    user = urllib.parse.quote_plus(user_raw)
    password = urllib.parse.quote_plus(password_raw)
    dbname = urllib.parse.quote_plus(dbname_raw)

    # Monta a URL final segura (password e user já estão escapados)
    url_conexao = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"

    print(f"Inspecionando a tabela '{nome_tabela}'...")
    
    resultado = obter_colunas(nome_tabela, url_conexao)
    
    print("-" * 30)
    if "erro" in resultado:
        print(resultado["erro"])
    else:
        for coluna, tipo in resultado.items():
            print(f"- {coluna}: {tipo}")
    print("-" * 30)

if __name__ == "__main__":
    main()