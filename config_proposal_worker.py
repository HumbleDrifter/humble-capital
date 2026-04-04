from env_runtime import load_runtime_env
from services.config_proposal_generation_service import main


load_runtime_env(override=True)


if __name__ == "__main__":
    main()
