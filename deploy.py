import os
import subprocess
import sys
import time

ENV_CONFIG = {
    "devel": {"stack_name": "MyLib-devel", "env_file": ".env"},
    "prod": {"stack_name": "MyLib", "env_file": ".env"},
}


def start_docker_and_wait():
    """
    Tenta iniciar o Docker Desktop no Windows e aguarda o daemon responder.
    """

    docker_path = r"C:\Program Files\Docker\Docker\Docker Desktop.exe"

    if not os.path.exists(docker_path):
        print(f"Executável do Docker não encontrado em: {docker_path}")
        print("Por favor, inicie o Docker manualmente.")
        return False

    print("Iniciando Docker Desktop...")
    try:
        subprocess.Popen(docker_path)
    except Exception as e:
        print(f"Erro ao tentar abrir o Docker: {e}")
        return False

    print("Aguardando o Docker Engine inicializar (isso pode levar alguns segundos)...")
    max_retries = 30

    for i in range(max_retries):
        try:
            subprocess.run(
                "docker info",
                shell=True,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return True
        except subprocess.CalledProcessError:
            time.sleep(2)
            print(f".", end="", flush=True)  # Feedback visual de carregamento

    print("\nTempo limite excedido. O Docker demorou muito para iniciar.")
    return False


def check_docker():
    """Verifica se o Docker Daemon está rodando."""
    try:
        subprocess.run(
            "docker info",
            shell=True,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        print("Erro: O Docker não está rodando. Tentando iniciar o Docker Desktop.")
        if not start_docker_and_wait():
            sys.exit(1)
    except Exception as e:
        print(f"Erro: Não foi possível verificar o Docker: {e}")
        sys.exit(1)


def load_env(env_file):
    env_vars = {}
    if os.path.exists(env_file):
        print(f"--- Carregando variaveis de: {env_file} ---")
        with open(env_file, "r") as f:
            for line in f:
                if line.strip() and not line.strip().startswith("#"):
                    try:
                        key, value = line.strip().split("=", 1)
                        env_vars[key] = value
                    except ValueError:
                        continue
    else:
        print(f"AVISO: Arquivo {env_file} não encontrado!")
    return env_vars


def deploy(target_env):
    config = ENV_CONFIG.get(target_env)
    env_vars = load_env(config["env_file"])
    try:
        _extracted_and_deploy(env_vars, target_env, config["stack_name"])
    except KeyError as e:
        print(f"Erro: Variável {e} não encontrada no arquivo {config['env_file']}")
    except subprocess.CalledProcessError as e:
        print(f"Erro durante a execução do SAM: {e}")


def _extracted_and_deploy(env_vars, target_env, stack_name):
    # Lista de overrides
    check_docker()
    overrides = [
        f"Env={target_env}",
        f"MyUserPoolId={env_vars['MY_USER_POOL_ID']}",
        f"MyAppClientId={env_vars['MY_APP_CLIENT_ID']}",
        f"SupabaseUrl={env_vars['SUPABASE_URL']}",
        f"SupabaseKey={env_vars['SUPABASE_KEY']}",
        f"MALClientId={env_vars['MAL_CLIENT_ID']}",
        f"MALClientSecret={env_vars['MAL_CLIENT_SECRET']}",
    ]

    overrides_str = " ".join(overrides)

    print(f"--- Construindo para {target_env} ---")
    build_cmd = "sam build --use-container --cached --parallel"
    subprocess.run(build_cmd, shell=True, check=True)

    print(f"--- Fazendo Deploy da Stack: {stack_name} ---")

    cmd = (
        f"sam deploy "
        f"--stack-name {stack_name} "
        f"--parameter-overrides {overrides_str} "
        f"--resolve-s3 "
        f"--capabilities CAPABILITY_IAM"
    )

    subprocess.run(cmd, shell=True, check=True)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python deploy.py [devel|prod]")
        sys.exit(1)

    ambiente = sys.argv[1].lower()

    if ambiente not in ["devel", "prod"]:
        print("Ambiente inválido. Use 'devel' ou 'prod'.")
        sys.exit(1)

    deploy(ambiente)
