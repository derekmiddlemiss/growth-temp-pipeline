import os

from decouple import Config, RepositoryEnv

deploy_environments = ["staging", "production"]
environment = os.getenv("DEPLOY_ENVIRONMENT")
environment = environment if environment in deploy_environments else "staging"
config = Config(RepositoryEnv(f".env.{environment}"))
