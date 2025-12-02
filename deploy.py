import os
import subprocess


def load_env():
    env_vars = {}
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            for line in f:
                if line.strip() and not line.strip().startswith("#"):
                    key, value = line.strip().split("=", 1)
                    env_vars[key] = value
    return env_vars


def deploy():
    env = load_env()

    try:
        _extracted_and_deploy(env)
    except KeyError as e:
        print(f"Erro: Variável {e} não encontrada no arquivo .env")
    except subprocess.CalledProcessError:
        print("Erro durante a execução do SAM.")


# TODO Rename this here and in `deploy`
def _extracted_and_deploy(env):
    overrides = [
        f"MyUserPoolId={env['MY_USER_POOL_ID']}",
        f"MyAppClientId={env['MY_APP_CLIENT_ID']}",
        f"SupabaseUrl={env['SUPABASE_URL']}",
        f"SupabaseKey={env['SUPABASE_KEY']}",
        f"TmdbApiKey={env['TMDB_API_KEY']}",
        f"SteamgriddbApiKey={env['STEAMGRIDDB_API_KEY']}",
        f"MALClientId={env['MAL_CLIENT_ID']}",
        f"MALClientSecret={env['MAL_CLIENT_SECRET']}",
    ]

    overrides_str = " ".join(overrides)

    print("Construindo (sam build)...")
    subprocess.run("sam build", shell=True, check=True)

    print("Fazendo Deploy (sam deploy)...")
    cmd = f"sam deploy --parameter-overrides {overrides_str}"
    subprocess.run(cmd, shell=True, check=True)


if __name__ == "__main__":
    deploy()
