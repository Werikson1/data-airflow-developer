ARG AIRFLOW_UID="50000"
FROM apache/airflow:2.6.3

# Install specific version of markupsafe
RUN pip install MarkupSafe==2.1.3

# Correção do pacote da azure
RUN pip uninstall  --yes azure-storage \
    && pip install -U azure-storage-blob apache-airflow-providers-microsoft-azure==1.1.0

# Para permitir executar os testes unitários
RUN pip install pytest

# user root
USER 0

# instalando gcloud cli https://cloud.google.com/sdk/docs/install-sdk#deb:~:text=apt%2Dget.-,Dica%20do%20Docker,-%3A%20se%20voc%C3%AA
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-cli -y

USER ${AIRFLOW_UID}
