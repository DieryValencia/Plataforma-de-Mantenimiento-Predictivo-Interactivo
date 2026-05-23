FROM rabbitmq:3.12-management

# ============================================================
# Instalar el plugin de Delayed Message Exchange
# ============================================================
RUN apt-get update && apt-get install -y curl && \
    curl -L -o /opt/rabbitmq/plugins/rabbitmq_delayed_message_exchange-3.12.0.ez \
    https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/v3.12.0/rabbitmq_delayed_message_exchange-3.12.0.ez && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Activar los plugins necesarios offline
RUN rabbitmq-plugins enable --offline rabbitmq_delayed_message_exchange rabbitmq_management

EXPOSE 5672 15672
