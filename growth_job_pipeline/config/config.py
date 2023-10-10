import os

from decouple import Config, RepositoryEnv

deploy_environments = ["staging", "production"]
environment = os.getenv("DEPLOY_ENVIRONMENT")
environment = environment if environment in deploy_environments else "staging"

dir_path = os.path.dirname(os.path.realpath(__file__))
config = Config(RepositoryEnv(os.path.join(dir_path, f".env.{environment}")))
# bit of a hack, but no API to set environment variable in decouple
config.repository.data["DEPLOY_ENVIRONMENT"] = environment
