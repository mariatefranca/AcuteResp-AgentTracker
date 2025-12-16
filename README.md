# AcuteResp-AgentTracker

## Getting Started

To deploy and manage this asset bundle, follow these steps:

### 1. Deployment

- Click the **deployment rocket** 🚀 in the left sidebar to open the **Deployments** panel, then click **Deploy**.

### 2. Running Jobs & Pipelines

- To run a deployed job or pipeline, hover over the resource in the **Deployments** panel and click the **Run** button.

### 3. Managing Resources

- Use the **Create** dropdown to add resources to the asset bundle.
- Click **Schedule** on a notebook within the asset bundle to create a **job definition** that schedules the notebook.

## Documentation

- For information on using **Databricks Asset Bundles in the workspace**, see: [Databricks Asset Bundles in the workspace](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- For details on the **Databricks Asset Bundles format** used in this asset bundle, see: [Databricks Asset Bundles Configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)

# Estrutura do Projeto

.
├── conf/                 # Arquivos de configuração.
├── reports/              # Relatórios diários de SRAG.
│   └── report.html       # Relatório final em HTML gerado para o dia.
├── scratch/              # Notebooks de exploração.
├── resources/            # Notebooks para exploração.
├── src/                 # Código-fonte principal da aplicação.
│   ├── agents/           # Agentes responsáveis pela execução do relatório.
│       └── agent.py                 # Código de estruturação do agente.
│       └── agent_environment.py     # Código para criação do serving endpoint.
│       └── deploy_agent.py          # Código para execução do deploy do agente.
│   ├── agent_config/           # Arquivos de configuração do agente.
│       └── callback_handler.py      # Código para captura de logs de eventos nas chamadas do agente.
│       └── prompt.py                # System prompt.
│   ├── elt/            # Extract, load and transform data.
│       └── extract_static_data.py     # Extração de dados antigos/estáticos.
│       └── extract_refreshing_data.py     # Extração de dados que são atualidos com frequência.
│       └── feature_engineering.py     # Tranformação de dados para  engenharia de features.
│   ├── tools/            # Ferramentas (tools) executadas pelos agentes.
│       └── metric_calculator.py     # Ferramenta que calcula as métricas de SRAG.
│       └── visual_generator.py     # Ferramenta que plota as visualizações do relatório.
│       └── web_news_searcher.py     # Ferramenta que recebe uma query do agente e faz uma busca de notícias na web.
│       └── database_searcher.py     # Agente/Ferramenta que faz spark queries nas tabelas para responder perguntas do usuário sobre os dados.
│       └── report_finder.py         # Ferramenta que busca se existe um relatório já gerado no dia atual.
│       └── report_assembler.py     # Ferramenta que recebe dicionários contendo notícias, métricas e visualizações e compila o relatório.
│   ├── utils/                          # Arquivos utilitários.
│       └── srag_report_template.html   # Template do relatório.
│       └── general_helpers.py          # Funções utilitárias.

│   └── graph.py          # Graph Workflow do langgraph
│   └── main.py           # Ponto de entrada para execução do pipeline.
│   └── old_main.py       # Ponto de entrada para execução do pipeline (sem langgraph).
├── .env.example        # Arquivo de exemplo para variáveis de ambiente.
├── requirements.txt    # Lista de dependências Python.
└── README.md           # Documentação do projeto.