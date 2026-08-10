# A Flight SQL client for the want-mode drill: it needs to connect both
# with and without a client certificate against the same server, which
# Windows curl cannot do (schannel refuses a private CA and will not
# load a client certificate from PEM).
FROM python:3.12-slim
RUN pip install --no-cache-dir pyarrow
