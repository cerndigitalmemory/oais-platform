## test SSO locally

Create a new SSO application with

Redirect URI(s):

- http://localhost/api/oidc/callback/
- http://localhost

and use it for local development

## Setting environment variables

Locally, you should edit .env.dev if you don't want git to keep bugging you about docker-compose.yml being changed. While deploying, those should go in values.yaml or in secrets.
